#include "config.h"
#include "source/extensions/filters/http/ring_buffer_cache/ring_buffer_cache.h"
// #include "source/common/protobuf/utility.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace RingBufferCache {

Http::FilterFactoryCb FilterFactory::createFilterFactoryFromProtoTyped(
    const ProtoConfig::RingBufferCacheConfig& proto_config,
    const std::string& /* stats_prefix */,
    Server::Configuration::FactoryContext& /* context */) { 

  {
    absl::WriterMutexLock lock(&factory_mutex_);
    if (!shared_state_) {
      shared_state_ = std::make_shared<SharedState>(proto_config);
    }
  }

  return [this_shared_state = this->shared_state_](Http::FilterChainFactoryCallbacks& callbacks) -> void {
    callbacks.addStreamFilter(
        std::make_shared<RingBufferCache>(this_shared_state));
  };
}

REGISTER_FACTORY(FilterFactory, Server::Configuration::NamedHttpFilterConfigFactory);

} // namespace RingBufferCache
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy