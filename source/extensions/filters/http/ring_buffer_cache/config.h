#pragma once

#include "source/extensions/filters/http/ring_buffer_cache/ring_buffer_cache.pb.h"
#include "source/extensions/filters/http/ring_buffer_cache/ring_buffer_cache.pb.validate.h"
#include "source/extensions/filters/http/common/factory_base.h"
#include "source/extensions/filters/http/ring_buffer_cache/ring_buffer_cache.h"

#include <memory>

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace RingBufferCache {

namespace ProtoConfig = envoy::extensions::filters::http::ring_buffer_cache::v3;

class FilterFactory : public Common::FactoryBase<ProtoConfig::RingBufferCacheConfig> {
public:

  FilterFactory() : FactoryBase("envoy.filters.http.ring_buffer_cache") {
    //shared_state_
  }

  Http::FilterFactoryCb createFilterFactoryFromProtoTyped(
      const ProtoConfig::RingBufferCacheConfig& proto_config,
      const std::string& stats_prefix,
      Server::Configuration::FactoryContext& context) override;

private:
  SharedStatePtr shared_state_;
  absl::Mutex factory_mutex_;
};

} // namespace RingBufferCache
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy