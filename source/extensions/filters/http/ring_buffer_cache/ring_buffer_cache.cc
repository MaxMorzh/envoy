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

std::string RingBufferCache::generateCacheKey(const Http::RequestHeaderMap& headers) {
    if (!headers.Host() || !headers.Path()) {
        return "invalid_key_missing_host_or_path";
    }
    return absl::StrCat(headers.getHostValue(), headers.getPathValue());
}

bool RingBufferCache::isResponseCacheable(const Http::ResponseHeaderMap& headers) {
    const Http::RequestHeaderMap* request_headers_ptr = decoder_callbacks_->streamInfo().getRequestHeaders();
    if (headers.getStatusValue() >= "200" && headers.getStatusValue() < "300") {
        if (request_headers_ptr && request_headers_ptr->getMethodValue() == Http::Headers::get().MethodValues.Get) {
            return true;
        }
    }
    return false;
}

void RingBufferCache::resetFilterState() {
    current_response_headers_.reset();
    current_response_body_.reset();
    current_response_trailers_.reset();
    response_is_cacheable_ = false;
    is_primary_fetcher_ = false;
    pending_cache_key_.reset();
}

//Send cached response to all waiting requests
void RingBufferCache::notifyWaiters(const std::string& key, SharedState::CallbackList& waiters) {
    ConstResponseHeaderMapPtr headers_to_send_sptr;
    OwnedBufferInstancePtr body_to_send_uptr;
    ConstResponseTrailerMapPtr trailers_to_send_sptr;
    bool data_found = false;

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
            data_found = true;
        }
    }

    if (data_found) {
        for (Http::StreamDecoderFilterCallbacks* waiter_cb : waiters) {
            if (waiter_cb) {
                auto headers_copy = Http::createHeaderMap<Http::ResponseHeaderMapImpl>(*headers_to_send_sptr);
                bool has_body = body_to_send_uptr && body_to_send_uptr->length() > 0;
                bool has_trailers = trailers_to_send_sptr && !trailers_to_send_sptr->empty();

                waiter_cb->encodeHeaders(std::move(headers_copy), !has_body && !has_trailers, "ring-buffer-cache-coalesced");

                if (has_body) {
                    Buffer::OwnedImpl body_copy;
                    body_copy.add(*body_to_send_uptr);
                    waiter_cb->encodeData(body_copy, !has_trailers);
                }
                if (has_trailers) {
                    auto trailers_copy = Http::createHeaderMap<Http::ResponseTrailerMapImpl>(*trailers_to_send_sptr);
                    waiter_cb->encodeTrailers(std::move(trailers_copy));
                }
            }
        }
    }
}

RingBufferCache::RingBufferCache(SharedStatePtr shared_state)
    : shared_state_(std::move(shared_state)) {}

//Remove stream from pending list if destroyed
void RingBufferCache::onDestroy() {
    if (!decoder_callbacks_ || !pending_cache_key_.has_value() || !shared_state_) {
        return;
    }

    uint64_t stream_id = decoder_callbacks_->streamId();
    const std::string& key = pending_cache_key_.value();

    absl::WriterMutexLock lock(&shared_state_->mutex_);
    auto pending_it = shared_state_->pending_requests_.find(key);
    if (pending_it != shared_state_->pending_requests_.end()) {
        auto& waiters = pending_it->second.callbacks_;
        waiters.remove_if([this](Http::StreamDecoderFilterCallbacks* cb_ptr) {
            return cb_ptr == this->decoder_callbacks_;
        });

        bool was_primary = pending_it->second.primary_fetcher_stream_id_.has_value() &&
                           pending_it->second.primary_fetcher_stream_id_.value() == stream_id;

        if (was_primary || (waiters.empty() && !pending_it->second.primary_fetcher_stream_id_.has_value())) {
            shared_state_->pending_requests_.erase(pending_it);
        }
    }
}

Http::FilterHeadersStatus RingBufferCache::decodeHeaders(Http::RequestHeaderMap& headers, bool) {
    is_primary_fetcher_ = false;

    if (headers.getMethodValue() != Http::Headers::get().MethodValues.Get) {
        return Http::FilterHeadersStatus::Continue;
    }

    std::string key = generateCacheKey(headers);
    if (key.rfind("invalid_key", 0) == 0) {
        return Http::FilterHeadersStatus::Continue;
    }

    bool cache_hit = false;
    auto status = Http::FilterHeadersStatus::Continue;

    {
        absl::WriterMutexLock lock(&shared_state_->mutex_);
        auto cache_it = shared_state_->cache_.find(key);

        if (cache_it != shared_state_->cache_.end() && !cache_it->second.empty()) {
            const auto& entry = cache_it->second.front();
            if (entry.headers_) {
                auto headers_copy = Http::createHeaderMap<Http::ResponseHeaderMapImpl>(*entry.headers_);
                Buffer::OwnedImpl body_copy;
                if (entry.body_) body_copy.add(*entry.body_);

                auto trailers_copy = entry.trailers_ ? Http::createHeaderMap<Http::ResponseTrailerMapImpl>(*entry.trailers_) : nullptr;

                decoder_callbacks_->encodeHeaders(std::move(headers_copy), entry.body_ == nullptr && entry.trailers_ == nullptr, "from_ring_buffer_cache_filter (shared_hit)");
                if (entry.body_) decoder_callbacks_->encodeData(body_copy, entry.trailers_ == nullptr);
                if (entry.trailers_) decoder_callbacks_->encodeTrailers(std::move(trailers_copy));

                return Http::FilterHeadersStatus::StopIteration;
            } else {
                shared_state_->cache_.erase(cache_it);
            }
        }

        auto pending_it = shared_state_->pending_requests_.find(key);
        if (pending_it != shared_state_->pending_requests_.end()) {
            pending_it->second.callbacks_.push_back(decoder_callbacks_);
            pending_cache_key_ = key;
            status = Http::FilterHeadersStatus::StopIteration;
        } else {
            shared_state_->pending_requests_[key].primary_fetcher_stream_id_ = decoder_callbacks_->streamId();
            is_primary_fetcher_ = true;
            pending_cache_key_ = key;
        }
    }

    return status;
}

Http::FilterHeadersStatus RingBufferCache::encodeHeaders(Http::ResponseHeaderMap& headers, bool) {
    resetFilterState();
    current_response_body_ = std::make_unique<Buffer::OwnedImpl>();
    response_is_cacheable_ = isResponseCacheable(headers);

    if (response_is_cacheable_) {
        current_response_headers_ = Http::createHeaderMap<Http::ResponseHeaderMapImpl>(headers);
    }

    return Http::FilterHeadersStatus::Continue;
}

Http::FilterDataStatus RingBufferCache::encodeData(Buffer::Instance& data, bool end_stream) {
    if (!response_is_cacheable_) {
        if (end_stream && pending_cache_key_.has_value() && is_primary_fetcher_) {
            processEndOfStream(pending_cache_key_.value(), false);
        }
        return Http::FilterDataStatus::Continue;
    }

    if (current_response_body_) {
        current_response_body_->add(data);
    }

    if (end_stream && pending_cache_key_.has_value() && is_primary_fetcher_) {
        processEndOfStream(pending_cache_key_.value(), true);
    }

    return Http::FilterDataStatus::Continue;
}

Http::FilterTrailersStatus RingBufferCache::encodeTrailers(Http::ResponseTrailerMap& trailers) {
    if (!response_is_cacheable_) {
        if (pending_cache_key_.has_value() && is_primary_fetcher_) {
            processEndOfStream(pending_cache_key_.value(), false);
        }
        return Http::FilterTrailersStatus::Continue;
    }

    if (current_response_headers_) {
        current_response_trailers_ = Http::createHeaderMap<Http::ResponseTrailerMapImpl>(trailers);
    }

    if (pending_cache_key_.has_value() && is_primary_fetcher_) {
        processEndOfStream(pending_cache_key_.value(), true);
    }

    return Http::FilterTrailersStatus::Continue;
}

Http::FilterTrailersStatus RingBufferCache::decodeTrailers(Http::RequestTrailerMap&) {
    return Http::FilterTrailersStatus::Continue;
}

//Finalize and notify waiters after response is complete
void RingBufferCache::processEndOfStream(const std::string& key, bool response_was_cacheable) {
    uint64_t stream_id = decoder_callbacks_ ? decoder_callbacks_->streamId() : 0;
    SharedState::CallbackList callbacks;

    {
        absl::WriterMutexLock lock(&shared_state_->mutex_);

        if (is_primary_fetcher_ && response_was_cacheable && current_response_headers_) {
            if (!current_response_body_) current_response_body_ = std::make_unique<Buffer::OwnedImpl>();
            CacheEntry new_entry(current_response_headers_, std::move(current_response_body_), current_response_trailers_);

            auto& buffer = shared_state_->cache_[key];
            buffer.push_front(std::move(new_entry));
            if (buffer.size() > shared_state_->buffer_size_) buffer.pop_back();
        }

        auto pending_it = shared_state_->pending_requests_.find(key);
        if (pending_it != shared_state_->pending_requests_.end()) {
            bool is_fetcher = pending_it->second.primary_fetcher_stream_id_.value_or(0) == stream_id;
            if (is_fetcher || !pending_it->second.primary_fetcher_stream_id_.has_value()) {
                callbacks = std::move(pending_it->second.callbacks_);
                shared_state_->pending_requests_.erase(pending_it);
            }
        }
    }

    if (!callbacks.empty()) {
        ConstResponseHeaderMapPtr headers;
        OwnedBufferInstancePtr body;
        ConstResponseTrailerMapPtr trailers;
        bool found_data = false;

        {
            absl::WriterMutexLock lock(&shared_state_->mutex_);
            auto cache_it = shared_state_->cache_.find(key);
            if (cache_it != shared_state_->cache_.end() && !cache_it->second.empty()) {
                const auto& entry = cache_it->second.front();
                if (entry.headers_) {
                    headers = entry.headers_;
                    if (entry.body_ && entry.body_->length() > 0) {
                        body = std::make_unique<Buffer::OwnedImpl>();
                        body->add(*entry.body_);
                    }
                    trailers = entry.trailers_;
                    found_data = true;
                }
            }
        }

        if (found_data) {
            notifyWaiters(key, callbacks, headers, body, trailers, found_data);
        }
    }

    resetFilterState();
}

//Actual implementation for notifying waiters with response parts
void RingBufferCache::notifyWaiters(const std::string& key, SharedState::CallbackList& waiters, ConstResponseHeaderMapPtr headers, const OwnedBufferInstancePtr& body, ConstResponseTrailerMapPtr trailers, bool data_found) {
    if (!data_found || !headers) return;

    for (Http::StreamDecoderFilterCallbacks* cb : waiters) {
        if (!cb) continue;

        auto headers_copy = Http::createHeaderMap<Http::ResponseHeaderMapImpl>(*headers);
        bool has_body = body && body->length() > 0;
        bool has_trailers = trailers && !trailers->empty();

        cb->encodeHeaders(std::move(headers_copy), !has_body && !has_trailers, "ring-buffer-cache-coalesced");

        if (has_body) {
            Buffer::OwnedImpl body_copy;
            body_copy.add(*body);
            cb->encodeData(body_copy, !has_trailers);
        }

        if (has_trailers) {
            auto trailers_copy = Http::createHeaderMap<Http::ResponseTrailerMapImpl>(*trailers);
            cb->encodeTrailers(std::move(trailers_copy));
        }
    }
}

} // namespace RingBufferCache
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
