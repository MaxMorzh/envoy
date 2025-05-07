#include "source/extensions/filters/http/ring_buffer_cache/ring_buffer_cache.h"

#include "source/common/buffer/buffer_impl.h"
#include "source/common/http/header_map_impl.h"
#include "source/common/http/utility.h"
#include "envoy/stream_info/stream_info.h"
#include "source/common/protobuf/utility.h" 

#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h" 

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace RingBufferCache {

//Definition
std::string RingBufferCache::generateCacheKey(const Http::RequestHeaderMap& headers) {
    if (!headers.Host() || !headers.Path()) {
        ENVOY_LOG(warn, "generateCacheKey post or path header is missing");
        return "invalid_key_missing_host_or_path";
    }
    return absl::StrCat(headers.getHostValue(), headers.getPathValue());
}

//Definition
RingBufferCache::RingBufferCache(SharedStatePtr shared_state)
    : shared_state_(std::move(shared_state)) { //shared state pointer
    ENVOY_LOG(debug, "RingBufferCache instance created");
    if (shared_state_ && shared_state_->buffer_size_ == 0) {
        ENVOY_LOG(warn, "RingBufferCache buffer_size in shared_state is 0");
    } else if (!shared_state_) {
        //не случалось
        ENVOY_LOG(error, "RingBufferCache shared_state_ is null during construction");
    }
}

//Definition
void RingBufferCache::onDestroy() {
    if (!decoder_callbacks_) {
        ENVOY_LOG(trace, "RingBufferCache::onDestroy called for stream before decoder_callbacks_ were set");
        return;
    }
    const uint64_t stream_id = decoder_callbacks_->streamId();
    ENVOY_LOG(debug, "[S{}] RingBufferCache::onDestroy START", stream_id);

    if (pending_cache_key_.has_value() && shared_state_) {
        const std::string& key = pending_cache_key_.value();
        ENVOY_LOG(debug, "[S{}] RingBufferCache::onDestroy ыtream was associated with key '{}'",
                  stream_id, key);
        absl::WriterMutexLock lock(&shared_state_->mutex_);
        auto pending_it = shared_state_->pending_requests_.find(key);
        if (pending_it != shared_state_->pending_requests_.end()) {
            auto& waiters = pending_it->second.callbacks_;
            size_t size_before = waiters.size();

            waiters.remove_if([this](Http::StreamDecoderFilterCallbacks* cb_ptr) {
                return cb_ptr == this->decoder_callbacks_;
            });
            size_t size_after = waiters.size();
            if (size_before > size_after) {
                 ENVOY_LOG(debug, "[S{}] RingBufferCache::onDestroy Removed callback from pending waiters list for key '{}'. Remaining waiters: {}",
                           stream_id, key, size_after);
            } else {
                 ENVOY_LOG(debug, "[S{}] RingBufferCache::onDestroy Callback not found in pending waiters list for key '{}' during cleanup",
                           stream_id, key);
            }

            if (pending_it->second.primary_fetcher_stream_id_.has_value() &&
                pending_it->second.primary_fetcher_stream_id_.value() == stream_id) {
                ENVOY_LOG(debug, "[S{}] RingBufferCache::onDestroy The primary fetcher for key '{}' is being destroyed", stream_id, key);
                shared_state_->pending_requests_.erase(pending_it);
            } else if (waiters.empty() && !pending_it->second.primary_fetcher_stream_id_.has_value()) {
                ENVOY_LOG(debug, "[S{}] RingBufferCache::onDestroy Last waiter for key '{}' destroyed, and no active primary fetcher",
                          stream_id, key);
                shared_state_->pending_requests_.erase(pending_it);
            }
        }
    }
     ENVOY_LOG(debug, "[S{}] RingBufferCache::onDestroy END", stream_id);
}


Http::FilterHeadersStatus RingBufferCache::decodeHeaders(Http::RequestHeaderMap& headers, bool /*end_stream*/) {
    if (!decoder_callbacks_ || !shared_state_) {
        ENVOY_LOG(warn, "decodeHeaders called before decoder_callbacks_ or shared_state_ set");
        return Http::FilterHeadersStatus::Continue;
    }
    const uint64_t stream_id = decoder_callbacks_->streamId();
    ENVOY_LOG(debug, "[S{}] decodeHeaders START", stream_id);
    is_primary_fetcher_ = false;

    if (headers.getMethodValue() != Http::Headers::get().MethodValues.Get) {
         ENVOY_LOG(debug, "[S{}] decodeHeaders Non-GET", stream_id);
         return Http::FilterHeadersStatus::Continue;
    }
    const std::string key = this->generateCacheKey(headers);
    //const std::string key = generateCacheKey(headers);

    if (key.rfind("invalid_key", 0) == 0) {
        ENVOY_LOG(warn, "[S{}] decodeHeaders: Could not generate valid cache key", stream_id);
        return Http::FilterHeadersStatus::Continue;
    }
    ENVOY_LOG(debug, "[S{}] decodeHeaders Cache key: {}", stream_id, key);

    Http::ResponseHeaderMapPtr mutable_headers_copy_cache_hit;
    Buffer::OwnedImpl body_copy_cache_hit;
    Http::ResponseTrailerMapPtr mutable_trailers_copy_cache_hit;
    bool has_body_cache_hit = false;
    bool has_trailers_cache_hit = false;
    bool serve_from_cache = false;
    Http::FilterHeadersStatus status_to_return = Http::FilterHeadersStatus::Continue;

    {
        absl::WriterMutexLock lock(&shared_state_->mutex_);
        ENVOY_LOG(trace, "[S{}] decodeHeaders Acquired lock for key '{}'", stream_id, key);

        auto cache_it = shared_state_->cache_.find(key);
        if (cache_it != shared_state_->cache_.end() && !cache_it->second.empty()) {
            const CacheEntry& entry = cache_it->second.front();
            if (!entry.headers_) {
                 ENVOY_LOG(warn, "[S{}] decodeHeaders Cache hit found null headers for key '{}'",
                           stream_id, key);
                 shared_state_->cache_.erase(cache_it);
            } else {
                mutable_headers_copy_cache_hit = Http::createHeaderMap<Http::ResponseHeaderMapImpl>(*entry.headers_);
                if (entry.body_ && entry.body_->length() > 0) {
                    body_copy_cache_hit.add(*entry.body_);
                    has_body_cache_hit = true;
                }
                if (entry.trailers_ && !entry.trailers_->empty()) {
                    mutable_trailers_copy_cache_hit = Http::createHeaderMap<Http::ResponseTrailerMapImpl>(*entry.trailers_);
                    has_trailers_cache_hit = true;
                }
                serve_from_cache = true;
                status_to_return = Http::FilterHeadersStatus::StopIteration;
                ENVOY_LOG(debug, "[S{}] decodeHeaders - Marked for cache serve for key '{}'", stream_id, key);
            }
        }

        if (!serve_from_cache) {
            ENVOY_LOG(debug, "[S{}] decodeHeaders - Cache miss for key '{}'.", stream_id, key);
            auto pending_it = shared_state_->pending_requests_.find(key);
            if (pending_it != shared_state_->pending_requests_.end()) {
                ENVOY_LOG(debug, "[S{}] decodeHeaders - Request for key '{}' added to pending list (primary S{}).",
                    stream_id, key, pending_it->second.primary_fetcher_stream_id_.value_or(0));
                pending_it->second.callbacks_.push_back(decoder_callbacks_);
                pending_cache_key_ = key;
                status_to_return = Http::FilterHeadersStatus::StopIteration;
            } else {
                ENVOY_LOG(debug, "[S{}] decodeHeaders - Cache miss for key '{}'", stream_id, key);
                shared_state_->pending_requests_[key].primary_fetcher_stream_id_ = stream_id;
                is_primary_fetcher_ = true;
                pending_cache_key_ = key;
                status_to_return = Http::FilterHeadersStatus::Continue;
            }
        }
        ENVOY_LOG(trace, "[S{}] decodeHeaders - Releasing lock for key '{}'", stream_id, key);
    }

    if (serve_from_cache) {
        ASSERT(decoder_callbacks_);
        decoder_callbacks_->encodeHeaders(std::move(mutable_headers_copy_cache_hit), !has_body_cache_hit && !has_trailers_cache_hit, "from_ring_buffer_cache_filter (shared_hit)");
        if (has_body_cache_hit) {
            decoder_callbacks_->encodeData(body_copy_cache_hit, !has_trailers_cache_hit);
        }
        if (has_trailers_cache_hit) {
             decoder_callbacks_->encodeTrailers(std::move(mutable_trailers_copy_cache_hit));
        }
        ENVOY_LOG(debug, "[S{}] decodeHeaders - Finished encoding from cache for key '{}'.", stream_id, key);
    }

    ENVOY_LOG(debug, "[S{}] decodeHeaders END returning status: {} for key '{}'", stream_id, static_cast<int>(status_to_return), key);
    return status_to_return;
}


Http::FilterHeadersStatus RingBufferCache::encodeHeaders(Http::ResponseHeaderMap& headers, bool /*end_stream*/) {
    if (!encoder_callbacks_ || !decoder_callbacks_ || !shared_state_) {
        ENVOY_LOG(warn, "encodeHeaders called before callbacks or shared_state_ set");
        return Http::FilterHeadersStatus::Continue;
    }
    const uint64_t stream_id = decoder_callbacks_->streamId();
    ENVOY_LOG(debug, "[S{}] encodeHeaders START", stream_id);

    current_response_headers_.reset();
    current_response_body_ = std::make_unique<Buffer::OwnedImpl>();
    current_response_trailers_.reset();
    response_is_cacheable_ = false;

    const Http::RequestHeaderMap* request_headers_ptr = decoder_callbacks_->streamInfo().getRequestHeaders();
    if (headers.getStatusValue() >= "200" && headers.getStatusValue() < "300") {
         if (request_headers_ptr && request_headers_ptr->getMethodValue() == Http::Headers::get().MethodValues.Get) {
              response_is_cacheable_ = true;
         }
    }

    if (!response_is_cacheable_) {
         ENVOY_LOG(debug, "[S{}] encodeHeaders - Response not cacheable (status: {}, method: {})",
            stream_id, headers.getStatusValue(), request_headers_ptr ? request_headers_ptr->getMethodValue() : absl::string_view("N/A"));
         return Http::FilterHeadersStatus::Continue;
    }

    ENVOY_LOG(debug, "[S{}] encodeHeaders - Response is cacheable", stream_id);
    current_response_headers_ = Http::createHeaderMap<Http::ResponseHeaderMapImpl>(headers);
    ENVOY_LOG(debug, "[S{}] encodeHeaders END", stream_id);
    return Http::FilterHeadersStatus::Continue;
}

Http::FilterDataStatus RingBufferCache::encodeData(Buffer::Instance& data, bool end_stream) {
    if (!encoder_callbacks_ || !decoder_callbacks_ || !shared_state_) {
        ENVOY_LOG(warn, "encodeData called before callbacks or shared_state_ set");
        return Http::FilterDataStatus::Continue;
    }
    const uint64_t stream_id = decoder_callbacks_->streamId();
    ENVOY_LOG(debug, "[S{}] encodeData START, data_length: {} end_stream: {}", stream_id, data.length(), end_stream);

    if (!response_is_cacheable_) {
        ENVOY_LOG(debug, "[S{}] encodeData - Skipping data accumulation, response not cacheable", stream_id);
        if (end_stream && pending_cache_key_.has_value() && is_primary_fetcher_) {
            processEndOfStream(pending_cache_key_.value(), false /* response_was_cacheable_and_cached */);
        }
        return Http::FilterDataStatus::Continue;
    }
    ASSERT(current_response_headers_ && "Cacheable response should have headers stored");
    ASSERT(current_response_body_ && "Cacheable response should have body_buffer initialized");
    current_response_body_->add(data);
    if (end_stream) {
        ENVOY_LOG(debug, "[S{}] encodeData End of stream", stream_id);
        if (pending_cache_key_.has_value() && is_primary_fetcher_) {
             processEndOfStream(pending_cache_key_.value(), true /* response_was_cacheable_and_cached */);
        } else {
            ENVOY_LOG(debug, "[S{}] encodeData End of stream, but not primary fetcher or no pending_key", stream_id);
        }
    }
    ENVOY_LOG(debug, "[S{}] encodeData END", stream_id);
    return Http::FilterDataStatus::Continue;
}

Http::FilterTrailersStatus RingBufferCache::encodeTrailers(Http::ResponseTrailerMap& trailers) {
    if (!encoder_callbacks_ || !decoder_callbacks_ || !shared_state_) {
        ENVOY_LOG(warn, "encodeTrailers called before callbacks or shared_state_ set");
        return Http::FilterTrailersStatus::Continue;
    }
    const uint64_t stream_id = decoder_callbacks_->streamId();
    ENVOY_LOG(debug, "[S{}] encodeTrailers START", stream_id);
    if (!response_is_cacheable_) {
        ENVOY_LOG(debug, "[S{}] encodeTrailers - Not cacheable", stream_id);
        if (pending_cache_key_.has_value() && is_primary_fetcher_) {
            processEndOfStream(pending_cache_key_.value(), false);
        }
        return Http::FilterTrailersStatus::Continue;
    }
    ASSERT(current_response_headers_);
    current_response_trailers_ = Http::createHeaderMap<Http::ResponseTrailerMapImpl>(trailers);
    if (pending_cache_key_.has_value() && is_primary_fetcher_) {
        processEndOfStream(pending_cache_key_.value(), true);
    } else {
        ENVOY_LOG(debug, "[S{}] encodeTrailers - End of stream (trailers), but not primary fetcher or no pending_key", stream_id);
    }
    ENVOY_LOG(debug, "[S{}] encodeTrailers END", stream_id);
    return Http::FilterTrailersStatus::Continue;
}

Http::FilterTrailersStatus RingBufferCache::decodeTrailers(Http::RequestTrailerMap& /*trailers*/) {
     ENVOY_LOG(trace, "[S{}] decodeTrailers Continue", decoder_callbacks_ ? decoder_callbacks_->streamId() : 0);
     return Http::FilterTrailersStatus::Continue;
}


void RingBufferCache::processEndOfStream(const std::string& key, bool response_was_cacheable_and_cached) {
    const uint64_t original_stream_id = decoder_callbacks_ ? decoder_callbacks_->streamId() : 0;
    ENVOY_LOG(debug, "[Original S{}] processEndOfStream START for key '{}' Response cacheable and cached: {}",
              original_stream_id, key, response_was_cacheable_and_cached);

    SharedState::CallbackList callbacks_to_notify;
    ConstResponseHeaderMapPtr headers_to_send_sptr;
    OwnedBufferInstancePtr body_to_send_uptr;
    ConstResponseTrailerMapPtr trailers_to_send_sptr;
    bool retrieved_data_for_waiters = false;

    {
        absl::WriterMutexLock lock(&shared_state_->mutex_);
        ENVOY_LOG(trace, "[Original S{}] processEndOfStream - Acquired shared lock for key '{}'", original_stream_id, key);

        // Caching
        if (is_primary_fetcher_ && response_was_cacheable_and_cached && current_response_headers_) {
            ENVOY_LOG(debug, "[Original S{}] Caching response for key '{}'", original_stream_id, key);
            if (!current_response_body_) current_response_body_ = std::make_unique<Buffer::OwnedImpl>();

            CacheEntry new_entry(current_response_headers_, std::move(current_response_body_), current_response_trailers_);
            auto& buffer = shared_state_->cache_[key];
            buffer.push_front(std::move(new_entry));

            if (buffer.size() > shared_state_->buffer_size_) {
                ENVOY_LOG(trace, "[Original S{}] Cache full for key '{}', removing oldest (size {})", original_stream_id, key, buffer.size());
                buffer.pop_back();
            }
            ENVOY_LOG(debug, "[Original S{}] Response cached. Cache size for key '{}': {}",
                      original_stream_id, key, buffer.size());
        } else if (is_primary_fetcher_) {
             ENVOY_LOG(debug, "[Original S{}] Primary fetcher completed but response not cached for key '{}'. Cacheable flag: {}, Headers present: {}",
                       original_stream_id, key, response_is_cacheable_, current_response_headers_ != nullptr);
        }

        auto pending_it = shared_state_->pending_requests_.find(key);
        if (pending_it != shared_state_->pending_requests_.end()) {
             ENVOY_LOG(debug, "[Original S{}] Found pending request entry for key '{}'. Primary fetcher: S{}. Waiters: {}",
                       original_stream_id, key,
                       pending_it->second.primary_fetcher_stream_id_.value_or(0),
                       pending_it->second.callbacks_.size());

            bool this_stream_was_the_fetcher = pending_it->second.primary_fetcher_stream_id_.has_value() &&
                                              pending_it->second.primary_fetcher_stream_id_.value() == original_stream_id;

            if(this_stream_was_the_fetcher || !pending_it->second.primary_fetcher_stream_id_.has_value()){
                auto cache_it = shared_state_->cache_.find(key);
                if (cache_it != shared_state_->cache_.end() && !cache_it->second.empty()) {
                    const auto& cached_entry = cache_it->second.front();
                    if (cached_entry.headers_) {
                        headers_to_send_sptr = cached_entry.headers_;
                        if (cached_entry.body_ && cached_entry.body_->length() > 0) {
                            body_to_send_uptr = std::make_unique<Buffer::OwnedImpl>();
                            body_to_send_uptr->add(*cached_entry.body_);
                        }
                        trailers_to_send_sptr = cached_entry.trailers_; 
                        retrieved_data_for_waiters = true;
                        ENVOY_LOG(debug, "[Original S{}] Prepared data from cache for {} waiters for key '{}'",
                                  original_stream_id, pending_it->second.callbacks_.size(), key);
                    } else {
                         ENVOY_LOG(error, "[Original S{}] Found null headers in cache entry for key '{}' while preparing data for waiters", original_stream_id, key);
                    }
                } else {
                     ENVOY_LOG(warn, "[Original S{}] No cache entry found for key '{}' to serve {} waiters",
                               original_stream_id, key, pending_it->second.callbacks_.size());
                }

                callbacks_to_notify = std::move(pending_it->second.callbacks_); 
                shared_state_->pending_requests_.erase(pending_it);
                ENVOY_LOG(debug, "[Original S{}] Cleared pending request entry for key '{}' (was primary: {}) Notifying {} callbacks",
                          original_stream_id, key, this_stream_was_the_fetcher, callbacks_to_notify.size());
            } else {
                 ENVOY_LOG(warn, "[Original S{}] This stream completed for key '{}', but it was not the designated primary fetcher (expected S{})",
                           original_stream_id, key, pending_it->second.primary_fetcher_stream_id_.value_or(0));
            }
        } else {
             ENVOY_LOG(debug, "[Original S{}] No pending requests found for key '{}'", original_stream_id, key);
        }
        ENVOY_LOG(trace, "[Original S{}] processEndOfStream - Releasing shared lock for key '{}'", original_stream_id, key);
    } 

    //Notification logic
    if (!callbacks_to_notify.empty()) {
        if (retrieved_data_for_waiters && headers_to_send_sptr) {
            ENVOY_LOG(debug, "[Original S{}] Notifying {} pending callbacks for key '{}'", original_stream_id, callbacks_to_notify.size(), key);
            for (Http::StreamDecoderFilterCallbacks* waiter_cb : callbacks_to_notify) {
                 if (waiter_cb) { 
                     const uint64_t waiter_stream_id = waiter_cb->streamId();
                     ENVOY_LOG(debug, "[Original S{}] Notifying waiter S{} for key '{}'", original_stream_id, waiter_stream_id, key);

                     Http::ResponseHeaderMapPtr headers_copy = Http::createHeaderMap<Http::ResponseHeaderMapImpl>(*headers_to_send_sptr);
                     bool has_body_to_send = (body_to_send_uptr && body_to_send_uptr->length() > 0);
                     bool has_trailers_to_send = (trailers_to_send_sptr && !trailers_to_send_sptr->empty());

                     waiter_cb->encodeHeaders(std::move(headers_copy), !has_body_to_send && !has_trailers_to_send, "ring-buffer-cache-coalesced");

                     if (has_body_to_send) {
                        Buffer::OwnedImpl body_copy_for_waiter;
                        body_copy_for_waiter.add(*body_to_send_uptr);
                        waiter_cb->encodeData(body_copy_for_waiter, !has_trailers_to_send);
                     }
                     if (has_trailers_to_send) {
                         Http::ResponseTrailerMapPtr trailers_copy = Http::createHeaderMap<Http::ResponseTrailerMapImpl>(*trailers_to_send_sptr);
                         waiter_cb->encodeTrailers(std::move(trailers_copy));
                     }
                     ENVOY_LOG(trace, "[Original S{}] Finished notifying waiter S{}", original_stream_id, waiter_stream_id);
                 } else {
                     ENVOY_LOG(warn, "[Original S{}] processEndOfStream - Skipping notification for a null callback pointer found in pending list for key '{}'",
                               original_stream_id, key);
                 }
            }
        } else {
             ENVOY_LOG(error, "[Original S{}] No valid cached data to send to {} pending callbacks for key '{}'",
                       original_stream_id, callbacks_to_notify.size(), key);
        }
    }
    current_response_headers_.reset();
    current_response_body_.reset();
    current_response_trailers_.reset();
    response_is_cacheable_ = false;
    is_primary_fetcher_ = false;
    pending_cache_key_.reset();
    ENVOY_LOG(debug, "[Original S{}] processEndOfStream END for key '{}'.", original_stream_id, key);
}

} // namespace RingBufferCache
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
