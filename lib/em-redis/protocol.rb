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
      type, args = reply_type_and_args(line)
      response = send("type_#{handler_method_name(type)}", args)
      if response_incomplete?
        next
      else
        dispatch_response(response)
      end
    end
  end

  private

  def response_incomplete?
    bulk? or multi_bulk?
  end

  def bulk?
    @bulk
  end

  def multi_bulk?
    @multi_bulk and @multi_values.size < @multi_argc
  end

  def dispatch_response(response)
    @multi_bulk = false
    @callback.call(response)
  end

  def type_minus(args)
    args
  end

  def type_plus(args)
    args
  end

  def type_colon(args)
    args.to_i
  end

  def type_dollar(args)
    data_length = Integer(args)
    if -1 == data_length
      @multi_bulk ? (@multi_values << nil) : nil
    else
      @bulk = true
    end
  end

  def type_bulk(args)
    @multi_bulk ? (@multi_values << args) : args
  end

  def type_asterisk(args)
    argc = Integer(args)
    case argc
    when 0  then []
    when -1 then nil
    else initialize_multi_bulk(argc)
    end
  end

  def handler_method_name(type)
    self.class.constants.select do |const|
      self.class.const_get(const) == type
    end.reduce.to_s.downcase
  end

  def reply_type_and_args(line)
    if @bulk
      @bulk = false
      [BULK, line[0..-3]]
    else
      [line[0], line[1..-3]]
    end
  end

  def initialize_multi_bulk(argc)
    @multi_bulk   = true
    @multi_values = []
    @multi_argc   = argc
  end

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
