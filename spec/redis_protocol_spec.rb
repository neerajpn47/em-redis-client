require 'eventmachine'
require 'rspec'

module EM::P::Redis
  
  COLON     = ":".freeze
  MINUS     = "-".freeze
  PLUS      = "+".freeze
  DOLLAR    = "$".freeze
  ASTERISK  = "*".freeze
  DELIMITER = "\r\n".freeze

  def self.connect(options = {})
    host = options[:host] || 'localhost'
    port = (options[:port] || 6379).to_i
    EM.connect host, port, self
  end

  def send_request(line, &blk)
    @request_cb = blk
    send_data format_as_multi_bulk_reply(line)
  end

  def receive_data(reply)
    reply_type = reply[0]
    reply_args = reply[1..-3]
    response = case reply_type
               when PLUS, MINUS then reply_args
               when COLON       then reply_args.to_i
               end
    @request_cb.call(response)
  end
  
  private

  def format_as_multi_bulk_reply(line)
    words = line.split
    prefix = multi_bulk_reply_prefix_from(words.size)
    words.inject(prefix) do |reply, each_word|
      reply += format_as_bulk_reply(each_word)
    end
  end

  def multi_bulk_reply_prefix_from(argc)
    ASTERISK    +
      argc.to_s + 
      DELIMITER
  end
  
  def format_as_bulk_reply(word)
    DOLLAR               +
      word.bytesize.to_s +
      DELIMITER          +
      word               +
      DELIMITER
  end
end

EM.run {
  redis = EM::P::Redis.connect
  redis.send_request("foobar") { |r| p r }
  EM.stop
}


class TestConnection
  include EM::P::Redis

  def send_data(data)
    data
  end
end

describe EM::P::Redis do
  before(:each) do
    @c = TestConnection.new
  end

  it { should respond_to(:connect) }

  describe "Requests" do
    it "should send a request in the multi-bulk reply format" do
      request = "SET mykey myvalue"
      encoded_request = "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n"
      @c.send_request(request).should == encoded_request
    end
  end

  describe "Replies" do
    describe "which fit in single line" do
      it "should return status" do
        @c.send_request("PING") do |r|
          r.should == "PONG"
        end
        @c.receive_data "+PONG\r\n"
      end

      it "should return error" do
        @c.send_request("foobar") do |r|
          r.should == "ERR unknown command 'foobar'"
        end
        @c.receive_data "-ERR unknown command 'foobar'\r\n"
      end

      it "should return an integer" do
        @c.send_request("INCR foobar") do |r|
          r.should == 5
        end
        @c.receive_data ":5\r\n"
      end
    end

    describe "bulk replies" do
    end
  end

end
