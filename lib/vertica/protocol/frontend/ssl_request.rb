module Vertica
  module Protocol
    class SslRequest < FrontendMessage
      message_id nil

      def to_bytes
        [80877103].pack('N')
      end
    end
  end
end
