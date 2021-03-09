module RailsSqlViews
  module SchemaDumper
    def self.prepended(base)
      # A list of views which should not be dumped to the schema.
      # Acceptable values are strings as well as regexp.
      # This setting is only used if ActiveRecord::Base.schema_format == :ruby
      base.cattr_accessor :ignore_views
      base.ignore_views = []
      # Optional: specify the order that in which views are created.
      # This allows views to depend on and include fields from other views.
      # It is not necessary to specify all the view names, just the ones that
      # need to be created first
      base.cattr_accessor :view_creation_order
      base.view_creation_order = []
    end

    # Add views to the end of the dump stream
    def dump(stream)
      header(stream)
      extensions(stream)
      tables(stream)
      begin
        if @connection.supports_views?
          views(stream)
        end
      rescue => e
        if ActiveRecord::Base.logger
          ActiveRecord::Base.logger.error "Unable to dump views: #{e}"
        else
          raise e
        end
      end
      trailer(stream)
      stream
    end

    # Add views to the stream
    def views(stream)
      if view_creation_order.empty?
        sorted_views = @connection.views.sort
      else
        # set union, merge by joining arrays, removing dups
        # this will float the view name sin view_creation_order to the top
        # without requiring all the views to be specified
        sorted_views = view_creation_order | @connection.views
      end
      sorted_views.each do |v|
        next if [ActiveRecord::SchemaMigration.table_name, ignore_views].flatten.any? do |ignored|
          case ignored
          when String then v == ignored
          when Symbol then v == ignored.to_s
          when Regexp then v =~ ignored
          else
            raise StandardError, 'ActiveRecord::SchemaDumper.ignore_views accepts an array of String and / or Regexp values.'
          end
        end
        view(v, stream)
      end
    end

    # Add the specified view to the stream
    def view(view, stream)
      columns = @connection.columns(view).collect { |c| c.name }
      begin
        v = StringIO.new

        v.print "  create_view #{view.inspect}"
        v.print ", #{@connection.view_select_statement(view).dump}"
        v.print ", :force => true"
        v.puts " do |v|"

        columns.each do |column|
          v.print "    v.column :#{column}"
          v.puts
        end

        v.puts "  end"
        v.puts

        v.rewind
        stream.print v.read
      rescue => e
        stream.puts "# Could not dump view #{view.inspect} because of following #{e.class}"
        stream.puts "#   #{e.message}"
        stream.puts
      end

      stream
    end

    def tables(stream)
      @connection.base_tables.sort.each do |tbl|
        next if [ActiveRecord::SchemaMigration.table_name, ignore_tables].flatten.any? do |ignored|
          case ignored
          when String then tbl == ignored
          when Regexp then tbl =~ ignored
          else
            raise StandardError, 'ActiveRecord::SchemaDumper.ignore_tables accepts an array of String and / or Regexp values.'
          end
        end
        table(tbl, stream)
      end
    end
  end
end
