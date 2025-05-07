load(
    "//bazel:envoy_build_system.bzl",
    "envoy_cc_extension",
    "envoy_cc_library",
    "envoy_extension_package",
    "envoy_proto_library",
)

licenses(["notice"])  # Apache 2

envoy_extension_package()

envoy_proto_library(
    name = "ring_buffer_cache_proto",
    srcs = ["ring_buffer_cache.proto"],
)

envoy_cc_library(
    name = "ring_buffer_cache_lib",
    srcs = ["ring_buffer_cache.cc"],
    hdrs = ["ring_buffer_cache.h"],
    deps = [
        "//source/common/common:logger_lib",
        "//source/common/http:header_map_lib",
        "//source/common/buffer:buffer_lib",
        "//envoy/http:header_map_interface",
        "//envoy/http:filter_interface",
        "//envoy/server:filter_config_interface",
        "//source/common/http:utility_lib",
        "//envoy/stream_info:stream_info_interface",
        "//envoy/http:metadata_interface",
        "//source/common/protobuf:utility_lib",
        "//envoy/protobuf:message_validator_interface", 

        ":ring_buffer_cache_proto_cc_proto",

        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/strings",
    ],
)

envoy_cc_extension(
    name = "config_envoy_extension",
    srcs = ["config.cc"],
    hdrs = ["config.h"],
    deps = [
        ":ring_buffer_cache_lib",
        "//source/extensions/filters/http/common:factory_base_lib",
        ":ring_buffer_cache_proto_cc_proto",
        "@com_envoyproxy_protoc_gen_validate//validate:validate_cc",
    ],
)
