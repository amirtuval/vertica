# The PreparedQueryExecutor class handles the state of the connection while a prepared query is being executed.
#
# @note This class is for internal use only, you should never interact with this class directly.
#
# @see Vertica::Connection#prepare
# @see Vertica::PreparedQuery#execute
class Vertica::PreparedQueryExecutor

  attr_reader :connection, :sql, :row_handler, :copy_handler
  attr_accessor :error, :result, :row_description, :buffer
 
  include Vertica::QueryProcessor

  def initialize(connection, sql, prepared_query_name, row_description, parameter_types, parameter_values)
    @connection, @sql, @prepared_query_name, @row_description, @parameter_types, @parameter_values, @portal_name = 
      connection, sql, prepared_query_name, row_description, parameter_types, parameter_values, Time.now.strftime("%H%M%S%N")

    @connection.write_message(Vertica::Protocol::Bind.new(@portal_name, @prepared_query_name, @parameter_types, @parameter_values))
    @connection.write_message(Vertica::Protocol::Flush.new)
  end

  def run
    @connection.write_message(Vertica::Protocol::Execute.new(@portal_name, 0))
    @connection.write_message(Vertica::Protocol::Sync.new)
    @connection.write_message(Vertica::Protocol::Flush.new)

    process_backend_messages
  end

  def execute(&block)
    @row_handler = block || lambda { |row| buffer_row(row) }
    @copy_handler = nil
    @buffer = block.nil? ? [] : nil
    @error = nil

    @connection.send(:run_in_mutex, self)
  end

  def process_message(message)
    case message
      when Vertica::Protocol::BindComplete
        handle_bind_complete(message)
      when Vertica::Protocol::PortalSuspended
        handle_portal_suspended(message)
      else
        super(message)
    end
  end

  def handle_bind_complete(message)
  end

  def handle_portal_suspended(message)
    if buffer_rows?
      complete_operation('')
    else
      @result = nil
    end

  end

  def close
    @connection.write_message(Vertica::Protocol::Close.new(:portal, @portal_name))
    @connection.write_message(Vertica::Protocol::Flush.new)
  
    begin
      message = connection.read_message
    end until message.kind_of?(Vertica::Protocol::CloseComplete)

  end
end