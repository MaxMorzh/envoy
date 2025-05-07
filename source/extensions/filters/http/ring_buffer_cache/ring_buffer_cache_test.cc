#include "source/extensions/filters/http/ring_buffer_cache/ring_buffer_cache.h" // Your filter header
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "test/mocks/http/mocks.h"
#include "test/mocks/server/mocks.h" 
#include "test/mocks/stream_info/mocks.h" 
#include "test/mocks/buffer/mocks.h"
#include "test/test_common/utility.h"
#include "source/extensions/filters/http/ring_buffer_cache/ring_buffer_cache.pb.h"

using testing::_;
using testing::NiceMock;
using testing::Return;
using testing::ReturnRef; 
using testing::SaveArg;
using testing::Invoke;

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace RingBufferCache {

class RingBufferCacheTest : public testing::Test {
public:
    void SetUp() override {
        config_proto_.set_buffer_size(3); 

        filter_ = std::make_shared<RingBufferCache>(config_proto_);
        filter_->setDecoderFilterCallbacks(decoder_callbacks_);
        filter_->setEncoderFilterCallbacks(encoder_callbacks_);
        ON_CALL(decoder_callbacks_, streamInfo()).WillByDefault(ReturnRef(stream_info_));
        ON_CALL(stream_info_, getRequestHeaders()).WillByDefault(Return(&request_headers_));
    }
    void TearDown() override {
        // filter_->onDestroy();
    }

protected:
    envoy::extensions::filters::http::ring_buffer_cache::v3::RingBufferCacheConfig config_proto_;
    std::shared_ptr<RingBufferCache> filter_;

    NiceMock<Http::MockStreamDecoderFilterCallbacks> decoder_callbacks_;
    NiceMock<Http::MockStreamEncoderFilterCallbacks> encoder_callbacks_;
    NiceMock<StreamInfo::MockStreamInfo> stream_info_;
    Http::TestRequestHeaderMapImpl request_headers_{{":method", "GET"}, {":path", "/test"}, {":authority", "testhost"}};
    Http::TestResponseHeaderMapImpl response_headers_{{":status", "200"}};
};

TEST_F(RingBufferCacheTest, InitTest) {
    SUCCEED();
}

TEST_F(RingBufferCacheTest, CacheMiss) {
    EXPECT_EQ(Http::FilterHeadersStatus::Continue, filter_->decodeHeaders(request_headers_, false));
}

TEST_F(RingBufferCacheTest, CacheHit) {
    EXPECT_EQ(Http::FilterHeadersStatus::Continue, filter_->decodeHeaders(request_headers_, false));
    EXPECT_EQ(Http::FilterHeadersStatus::Continue, filter_->encodeHeaders(response_headers_, false));
    Buffer::OwnedImpl data_to_encode("cached_data");
    EXPECT_EQ(Http::FilterDataStatus::Continue, filter_->encodeData(data_to_encode, true));

    Http::TestRequestHeaderMapImpl second_request_headers = request_headers_;

    EXPECT_CALL(decoder_callbacks_, encodeHeaders_(_, _));
    EXPECT_CALL(decoder_callbacks_, encodeData(_, _));
    EXPECT_EQ(Http::FilterHeadersStatus::StopIteration, filter_->decodeHeaders(second_request_headers, false));
}

TEST_F(RingBufferCacheTest, PendingRequestDestroyed) {
    EXPECT_EQ(Http::FilterHeadersStatus::Continue, filter_->decodeHeaders(request_headers_, false));
    Http::TestRequestHeaderMapImpl second_request_headers = request_headers_;
    NiceMock<Http::MockStreamDecoderFilterCallbacks> second_decoder_callbacks;
    filter_->setDecoderFilterCallbacks(second_decoder_callbacks);
    EXPECT_EQ(Http::FilterHeadersStatus::StopIteration, filter_->decodeHeaders(second_request_headers, false));
    filter_->setDecoderFilterCallbacks(decoder_callbacks_);
    filter_->onDestroy();

    EXPECT_EQ(Http::FilterHeadersStatus::Continue, filter_->encodeHeaders(response_headers_, false));
    Buffer::OwnedImpl data_to_encode("data");

    // ------
    EXPECT_CALL(second_decoder_callbacks, encodeHeaders_(_, _));
    EXPECT_CALL(second_decoder_callbacks, encodeData(_, _));
    EXPECT_CALL(decoder_callbacks_, encodeHeaders_(_, _)).Times(0);
    EXPECT_CALL(decoder_callbacks_, encodeData(_, _)).Times(0);
    // -------------------------------
    EXPECT_EQ(Http::FilterDataStatus::Continue, filter_->encodeData(data_to_encode, true));

    // testing::Mock::VerifyAndClearExpectations(&decoder_callbacks_);
    // testing::Mock::VerifyAndClearExpectations(&second_decoder_callbacks);
}


} // namespace RingBufferCache
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy