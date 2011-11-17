require_relative 'spec_helper'

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

      describe "when the network output is buffered" do
        it "should return the correct multi bulk reply" do
          @c.send_request("GET mysortedlist") do |r|
            r.should == ["foo", nil, "bar"]
          end
          "*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n".each_byte { |byte| @c.receive_data(byte) }
        end
      end
    end
  end

end
