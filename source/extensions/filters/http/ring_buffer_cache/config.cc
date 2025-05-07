#include "config.h"
#include "source/extensions/filters/http/ring_buffer_cache/ring_buffer_cache.h"
// #include "source/common/protobuf/utility.h" // Only if you call MessageUtil::validate manually

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace RingBufferCache {

// FilterFactory constructor is now defined and initializes base in config.h

Http::FilterFactoryCb FilterFactory::createFilterFactoryFromProtoTyped(
    const ProtoConfig::RingBufferCacheConfig& proto_config,
    const std::string& /* stats_prefix */,
    Server::Configuration::FactoryContext& /* context */) { // context can be used for logger, stats scope for shared_state

  // Thread-safe initialization of shared_state_ for this factory instance
  {
    absl::WriterMutexLock lock(&factory_mutex_);
    if (!shared_state_) {
      // FactoryBase has already validated proto_config by this point.
      shared_state_ = std::make_shared<SharedState>(proto_config);
    }
    // NOTE: If envoy.yaml can define multiple instances of this filter type
    // with DIFFERENT proto configs, this simple `shared_state_` member in the factory
    // will be overwritten by the last one loaded if the factory is a true singleton.
    // For distinct shared states per distinct proto config, a more complex map keyed by
    // config hash might be needed in the factory, or rely on FactoryContext's ability
    // to provide truly distinct contexts if the factory is re-created.
    // However, Envoy usually creates one factory instance per registered filter name.
    // The config provided here is the specific one for this filter chain.
    // So, if this factory is used for multiple filter chains with different configs,
    // this simple shared_state_ will represent the state of the *first one loaded*
    // or the *last one configured* if createFilterFactoryFromProtoTyped is called multiple times
    // with different protos on the *same factory object*. This needs care.
    // A common robust pattern is to get a shared object from context.serverFactoryContext().singletonManager().
    // For now, this will create one shared_state per FilterFactory object based on the first proto_config it sees.
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