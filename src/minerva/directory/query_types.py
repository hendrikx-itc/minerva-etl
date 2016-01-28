# -*- coding: utf-8 -*-
from minerva.db.query import Column, As, Table, Call, Select, FromItem, \
    Eq, ands, And, Any, ArrayContains, Parenthesis


class Tag:
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return "Tag('{}')".format(self.name)


class Alias:
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return "Alias('{}')".format(self.name)


class Context:
    def __init__(self, tags):
        self.tags = tags

    def __repr__(self):
        return "Context({})".format(", ".join([t.name for t in self.tags]))

    def tag_names(self):
        return [t.name for t in self.tags]


class Query:
    def __init__(self, parts):
        self.parts = parts

    def __repr__(self):
        return "Query({})".format(self.parts)

    def compile(self):
        args = []
        criteria = []
        from_item = None

        for level, part in enumerate(self.parts):
            if isinstance(part, Context):
                entity_tags_table = As(
                    Table("directory", "entity_tags"),
                    "etags_{}".format(level)
                )
                if not from_item:
                    from_item = FromItem(entity_tags_table)

                col_id = Column("id")

                columns = [Call("array_agg", col_id)]

                tag_name_criterion = Eq(Call("lower", Column("name")), Any())

                sub_select = Select(columns).from_(
                    [Table("directory", "tag")]
                ).where_(tag_name_criterion)

                criterion = ArrayContains(
                    Column(entity_tags_table.alias, "tag_ids"),
                    Parenthesis(sub_select))

                entity_id_column = Column(entity_tags_table, "entity_id")

                criteria.append(criterion)

                tag_names = map(str.lower, part.tag_names())

                context_args = (tag_names,)
                args.extend(context_args)

            elif isinstance(part, Alias):
                alias_table = As(
                    Table("directory", "alias"),
                    "alias_{}".format(level)
                )
                specifier_criterion = Eq(
                    Column(alias_table, "name"),
                    part.name
                )
                entity_id_criterion = Eq(
                    entity_id_column,
                    Column(alias_table, "entity_id")
                )

                criterion = And(entity_id_criterion, specifier_criterion)
                from_item = from_item.join(alias_table, criterion)

        query = Select(
            [entity_id_column]
        ).from_(from_item).where_(ands(criteria))

        return query, args

    def execute(self, cursor):
        select, args = self.compile()

        select.execute(cursor, args)
