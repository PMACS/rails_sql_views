module RailsSqlViews
  module ConnectionAdapters
    module OracleEnhancedAdapter
      def supports_materialized_views?
        true
      end

      # Returns true as this adapter supports views.
      def supports_views?
        true
      end

      # Returns true as this adapter supports replacing views.
      def replaces_views?
        true
      end

      def base_tables(name = nil) #:nodoc:
        query = "SELECT TABLE_NAME FROM ALL_TABLES WHERE owner = SYS_CONTEXT('userenv', 'current_schema') " \
                "AND secondary = 'N'"
        result = execute(query, name)
        result[:rows]
      end
      alias nonview_tables base_tables

      def views(name = nil) #:nodoc:
        result = execute("SELECT VIEW_NAME FROM ALL_VIEWS WHERE owner = SYS_CONTEXT('userenv', 'current_schema')", name)
        result[:rows]
      end

      # Get the view select statement for the specified table.
      def view_select_statement(view, name = nil)
        query = "SELECT TEXT FROM ALL_VIEWS WHERE VIEW_NAME = '#{view}' AND owner = SYS_CONTEXT('userenv', 'current_schema')"
        result = execute(query, name)

        return result[:rows] unless result[:rows].empty?

        raise "No view called #{view} found"
      end
    end
  end
end
