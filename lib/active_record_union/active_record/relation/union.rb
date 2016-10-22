module ActiveRecord
  class Relation
    module Union

      SET_OPERATION_TO_AREL_CLASS = {
        union:     Arel::Nodes::Union,
        union_all: Arel::Nodes::UnionAll
      }

      def union(relation_or_where_arg, *args)
        set_operation(:union, relation_or_where_arg, *args)
      end

      def union_all(relation_or_where_arg, *args)
        set_operation(:union_all, relation_or_where_arg, *args)
      end

      private

      def set_operation(operation, relation_or_where_arg, *args)
        # https://github.com/brianhempel/active_record_union/pull/4/files
        # looks like this PR is good, it can support flat union
        others = if Relation === relation_or_where_arg
                  [relation_or_where_arg, *args]
                else
                  [@klass.where(relation_or_where_arg, *args)]
                end

        verify_relations_for_set_operation!(operation, self, *others)

        # Postgres allows ORDER BY in the UNION subqueries if each subquery is surrounded by parenthesis
        # but SQLite does not allow parens around the subqueries; you will have to explicitly do `relation.reorder(nil)` in SQLite
        queries = if Arel::Visitors::SQLite === self.connection.visitor
          [self.ast, *others.map(&:ast)]
        else
          [Arel::Nodes::Grouping.new(self.ast), *others.map{|other| Arel::Nodes::Grouping.new(other.ast)}]
        end

        arel_class = SET_OPERATION_TO_AREL_CLASS[operation]
        set = queries.reduce { |left, right| arel_class.new(left, right) }
        from = Arel::Nodes::TableAlias.new(set, @klass.arel_table.name)
        if ActiveRecord::VERSION::MAJOR >= 5
          relation             = @klass.unscoped.spawn
          relation.from_clause = UnionFromClause.new(from, nil, self.bound_attributes + others.map(&:bound_attributes))
        else
          relation             = @klass.unscoped.from(from)
          # self.arel.bind_values was add in following PR
          # https://github.com/brianhempel/active_record_union/pull/7/files
          # however, AR 3.2.xxx or Arel 3.0.3 doesn't have binding_values in Arel::TreeManager
          # https://github.com/rails/arel/blob/v3.0.3/lib/arel/tree_manager.rb
          # I can't reproduce the bug in that PR mentioned, so I add responde_to checking first
          relation.bind_values = (self.arel.respond_to?(:binding_values) ? self.arel.bind_values : [])
          relation.bind_values += self.bind_values
          relation.bind_values += others.map{|other| other.arel.respond_to?(:binding_values) ? other.arel.bind_values : []}.flatten
          relation.bind_values += others.map(&:bind_values).flatten
        end
        relation
      end

      def verify_relations_for_set_operation!(operation, *relations)
        includes_relations = relations.select { |r| r.includes_values.any? }

        if includes_relations.any?
          raise ArgumentError.new("Cannot #{operation} relation with includes.")
        end

        preload_relations = relations.select { |r| r.preload_values.any? }
        if preload_relations.any?
          raise ArgumentError.new("Cannot #{operation} relation with preload.")
        end

        eager_load_relations = relations.select { |r| r.eager_load_values.any? }
        if eager_load_relations.any?
          raise ArgumentError.new("Cannot #{operation} relation with eager load.")
        end
      end

      if ActiveRecord::VERSION::MAJOR >= 5
        class UnionFromClause < ActiveRecord::Relation::FromClause
          def initialize(value, name, bound_attributes)
            super(value, name)
            @bound_attributes = bound_attributes
          end

          def binds
            @bound_attributes
          end
        end
      end
    end # Union
  end # Relation
end # ActiveRecord