require 'rspec'

require_relative '../lib/em-redis'

class TestConnection
  include EM::P::Redis

  def send_data(data)
    data
  end
end
