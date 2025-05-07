#pragma once

#include <deque>
#include <list>
#include <memory>
#include <string>
#include <atomic>

#include "envoy/http/filter.h"
#include "envoy/http/header_map.h"
#include "envoy/server/filter_config.h" 
#include "source/common/common/logger.h"
#include "source/common/http/header_map_impl.h"
#include "source/common/buffer/buffer_impl.h"

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/optional.h"

#include "source/extensions/filters/http/ring_buffer_cache/ring_buffer_cache.pb.h"


namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace RingBufferCache {

using ProtoCommonConfig = envoy::extensions::filters::http::ring_buffer_cache::v3::RingBufferCacheConfig;

using ConstResponseHeaderMapPtr = std::shared_ptr<const Http::ResponseHeaderMap>;
using ConstResponseTrailerMapPtr = std::shared_ptr<const Http::ResponseTrailerMap>;
using OwnedBufferInstancePtr = std::unique_ptr<Buffer::OwnedImpl>;

class CacheEntry {
public:
    CacheEntry(ConstResponseHeaderMapPtr headers, OwnedBufferInstancePtr&& body,
               ConstResponseTrailerMapPtr trailers)
        : headers_(headers), body_(std::move(body)), trailers_(trailers) {}

    ConstResponseHeaderMapPtr headers_;
    OwnedBufferInstancePtr body_;
    ConstResponseTrailerMapPtr trailers_;
};

struct SharedState {
    using CallbackList = std::list<Http::StreamDecoderFilterCallbacks*>;
    struct PendingRequest {
        CallbackList callbacks_;
        absl::optional<uint64_t> primary_fetcher_stream_id_{};
    };

    using CacheBuffer = std::deque<CacheEntry>;
    using CacheMap = absl::flat_hash_map<std::string, CacheBuffer>;
    using PendingRequestsMap = absl::flat_hash_map<std::string, PendingRequest>;

    SharedState(const ProtoCommonConfig& config) : buffer_size_(config.buffer_size()) {
    }

    CacheMap cache_ ABSL_GUARDED_BY(mutex_);
    PendingRequestsMap pending_requests_ ABSL_GUARDED_BY(mutex_);
    absl::Mutex mutex_; //Mutex to protect cache and pending requests

    const uint32_t buffer_size_; //buffer size
};

using SharedStatePtr = std::shared_ptr<SharedState>;

class RingBufferCache : public Http::StreamFilter, Logger::Loggable<Logger::Id::filter> {
public:
    RingBufferCache(SharedStatePtr shared_state);

    void onDestroy() override;
    Http::FilterHeadersStatus decodeHeaders(Http::RequestHeaderMap& headers, bool end_stream) override;
    Http::FilterDataStatus decodeData(Buffer::Instance&, bool) override {
        return Http::FilterDataStatus::Continue;
    }
    Http::FilterTrailersStatus decodeTrailers(Http::RequestTrailerMap& trailers) override;
    void setDecoderFilterCallbacks(Http::StreamDecoderFilterCallbacks& callbacks) override {
        decoder_callbacks_ = &callbacks;
    }
    Http::Filter1xxHeadersStatus encode1xxHeaders(Http::ResponseHeaderMap& /*headers*/) override {
         return Http::Filter1xxHeadersStatus::Continue;
    }
    Http::FilterMetadataStatus encodeMetadata(Http::MetadataMap& /*metadata_map*/) override {
        return Http::FilterMetadataStatus::Continue;
    }
    Http::FilterHeadersStatus encodeHeaders(Http::ResponseHeaderMap& headers, bool end_stream) override;
    Http::FilterDataStatus encodeData(Buffer::Instance& data, bool end_stream) override;
    Http::FilterTrailersStatus encodeTrailers(Http::ResponseTrailerMap& trailers) override;
    void setEncoderFilterCallbacks(Http::StreamEncoderFilterCallbacks& callbacks) override {
        encoder_callbacks_ = &callbacks;
    }

private:
    // Pointer to the shared state object cache, pending requests, mutex
    SharedStatePtr shared_state_;

    Http::StreamDecoderFilterCallbacks* decoder_callbacks_{nullptr};
    Http::StreamEncoderFilterCallbacks* encoder_callbacks_{nullptr};

    ConstResponseHeaderMapPtr current_response_headers_;
    OwnedBufferInstancePtr current_response_body_;
    ConstResponseTrailerMapPtr current_response_trailers_;
    bool response_is_cacheable_{false};

    absl::optional<std::string> pending_cache_key_; 
    bool is_primary_fetcher_{false};

    std::string generateCacheKey(const Http::RequestHeaderMap& headers);
    void processEndOfStream(const std::string& key, bool response_was_cacheable_and_cached);
};

} // namespace RingBufferCache
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy