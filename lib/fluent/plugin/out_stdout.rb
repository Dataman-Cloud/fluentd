#
# Fluentd
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

module Fluent
  class StdoutOutput < Output
    Plugin.register_output('stdout', self)

    config_param :output_type, :default => 'json'

    config_param :port, :integer, :default => DEFAULT_LISTEN_PORT
    config_param :host, :string, :default => nil

    $server

    def configure(conf)
      super
      @formatter = Plugin.new_formatter(@output_type)
      @formatter.configure(conf)

      require 'net/telnet'
      $server = Net::Telnet::new('Host' => @host,
        'Port' => @port,
        'Telnetmode' => false)
    end

    def emit(tag, es, chain)
      es.each {|time,record|
        #$log.write "#{Time.at(time).localtime} #{tag}: #{@formatter.format(tag, time, record).chomp}\n"
        begin
          #$server.puts "#{@formatter.format(tag, time, record).chomp}"
          jsonstr = parse_value(@formatter.format(tag, time, record).chomp)
          message = parse_value(jsonstr['message'])
          log = message['log']
          time = message['time']
          containername = jsonstr['containername']
          uuid = jsonstr['uuid']
          $server.puts "#{time} #{uuid} #{containername} #{log}"
        rescue
          $server = Net::Telnet::new('Host' => @host,
            'Port' => @port,
            'Telnetmode' => false)
        end
      }
      #$log.flush

      chain.next
    end

    private

    def parse_value(value_str)
      if value_str.start_with?('{', '[')
        JSON.parse(value_str)
      else
        value_str
      end
    rescue => e
      log.warn "failed to parse #{value_str} as json. Assuming #{value_str} is a string", :error_class => e.class, :error => e.message
      value_str # emit as string
    end

  end
end
