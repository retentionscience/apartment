require 'apartment/adapters/abstract_adapter'

module Apartment
  module Tenant

    def self.mysql2_adapter(config)
      Apartment.use_schemas ?
        Adapters::Mysql2SchemaAdapter.new(config) :
        Adapters::Mysql2Adapter.new(config)
    end
  end

  module Adapters
    class Mysql2Adapter < AbstractAdapter

    protected

      def connect_to_new(tenant)
        return reset if tenant.nil?

        check_tenant_config!(tenant)
        if server_changed?(tenant)
          return super(tenant)
        else
          begin
            Apartment.connection.execute "use `#{environmentify(tenant)}`"
          rescue ActiveRecord::StatementInvalid => exception
            Apartment.connection.execute "use `#{environmentify(Apartment::Tenant.current)}`"
            raise_connect_error!(tenant, exception)
          end
        end
      end

      def check_tenant_config!(tenant)
        unless multi_tenantify(tenant)[:host].present?
          error_msg = "missing tenant #{tenant} db config!!!"
          raise error_msg
        end
      end

      def server_changed?(to_tenant)
        multi_tenantify(to_tenant)[:host] != Apartment.connection_config[:host]
      rescue
        return true
      end

      def rescue_from
        Mysql2::Error
      end
    end

    class Mysql2SchemaAdapter < AbstractAdapter
      def initialize(config)
        super

        @default_tenant = config[:database]
        reset
      end

      #   Reset current tenant to the default_tenant
      #
      def reset
        Apartment.connection.execute "use `#{default_tenant}`"
      end

    protected

      #   Connect to new tenant
      #
      def connect_to_new(tenant)
        return reset if tenant.nil?

        Apartment.connection.execute "use `#{environmentify(tenant)}`"

      rescue ActiveRecord::StatementInvalid => exception
        Apartment::Tenant.reset
        raise_connect_error!(tenant, exception)
      end

      def process_excluded_model(model)
        model.constantize.tap do |klass|
          # Ensure that if a schema *was* set, we override
          table_name = klass.table_name.split('.', 2).last

          klass.table_name = "#{default_tenant}.#{table_name}"
        end
      end

      def reset_on_connection_exception?
        true
      end
    end
  end
end
