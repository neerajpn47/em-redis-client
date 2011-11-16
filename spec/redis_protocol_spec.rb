require 'eventmachine'
require 'rspec'

module EM::P::Redis
  
  COLON     = ":".freeze
  MINUS     = "-".freeze
  PLUS      = "+".freeze
  DOLLAR    = "$".freeze
  ASTERISK  = "*".freeze
  DELIMITER = "\r\n".freeze
  BULK      = true.freeze

  def self.connect(options = {})
    host = options[:host] || 'localhost'
    port = (options[:port] || 6379).to_i
    EM.connect host, port, self
  end

  def send_request(line, &blk)
    @callback = blk
    send_data format_as_multi_bulk_reply(line)
  end

  def receive_data(reply)
    (@buffer ||= "") << reply
    while index = @buffer.index(DELIMITER)
      line = @buffer.slice!(0, index + DELIMITER.size)
      if @bulk
        type = BULK
        args = line[0..-3]
        @bulk = false
      else
        type = line[0]
        args = line[1..-3]
      end
      response = case type
                 when PLUS, MINUS then args
                 when COLON       then args.to_i
                 when DOLLAR      then
                   data_length = Integer(args)
                   if -1 == data_length
                     if @multi_bulk
                       @multi_values << nil
                       if @multi_values.size == @multi_argc
                         @multi_values
                       else
                         next
                       end
                     else
                       nil
                     end
                   else
                     @bulk = true
                     next
                   end
                 when BULK        then
                   if @multi_bulk
                     @multi_values << args
                     if @multi_values.size == @multi_argc
                       @multi_values
                     else
                       next
                     end
                   else
                     args
                   end
                 when ASTERISK    then
                   argc = Integer(args)
                   if 0 == argc
                     []
                   elsif -1 == argc
                     nil
                   else
                     @multi_bulk   = true
                     @multi_values = []
                     @multi_argc   = argc
                     next
                   end
                 else
                   type + args
                 end
      @callback.call(response)
    end
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

      describe "when network output is buffered" do
        it "should return the correct single line response" do
          @c.send_request("PING") do |r|
            r.should == "PONG"
          end
          "+PONG\r\n".each_byte { |byte| @c.receive_data(byte) }
        end
      end
    end

    describe "bulk replies" do
      it "should return nil when the value does not exist" do
        @c.send_request("GET nonexistingkey") do |r|
          r.should be_nil
        end
        @c.receive_data "$-1\r\n"
      end

      it "should return a single binary safe string" do
        @c.send_request("GET mykey") do |r|
          r.should == "foobar"
        end
        @c.receive_data "$6\r\nfoobar\r\n"
      end

      describe "when network output is buffered" do
        it "should return the correct bulk reply" do
          @c.send_request("GET mykey") do |r|
            r.should == "foobar"
          end
          "$6\r\nfoobar\r\n".each_byte { |byte| @c.receive_data(byte) }
        end
      end
    end

    describe "multiple bulk replies" do
      it "should return an empty list when the key does not exist" do
        @c.send_request("LRANGE nokey 0 1") do |r|
          r.should be_empty
        end
        @c.receive_data "*0\r\n"
      end

      it "should return nil when an error occurs" do
        @c.send_request("BLPOP key 1") do |r|
          r.should be_nil
        end
        @c.receive_data "*-1\r\n"
      end

      it "should return multiple values" do
        @c.send_request("LRANGE mylist 0 3") do |r|
          r.should == ["foo", "bar", "Hello", "World"]
        end
        @c.receive_data "*4\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$5\r\nHello\r\n$5\r\nWorld\r\n"
      end

      it "should properly handle presence of nil values" do
        @c.send_request("GET mysortedlist") do |r|
          r.should == ["foo", nil, "bar"]
        end
        @c.receive_data "*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n"
      end
    end
  end

end
