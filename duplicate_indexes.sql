WITH ind_col_data AS (
  -- Gather components of index columns
  SELECT
    attrelid,
    pg_get_indexdef(
      indexrelid,
      attnum,
      true
    ) AS attname,
    CASE
      WHEN opcdefault
        THEN NULL
      ELSE
        opcname
    END AS opcname,
    CASE collname
      WHEN 'default'
        THEN NULL
      ELSE
        collname
    END AS collname,
    CASE (
      pg_index.indoption[attnum - 1] & 1 /* DESC */,
      pg_index.indoption[attnum - 1] & 2 /* NULLS FIRST */
    )
      WHEN (1 /* DESC */, 2 /* NULLS FIRST */)
        THEN 'DESC'
      WHEN (1 /* DESC */, 0 /* NULLS LAST */)
        THEN 'DESC NULLS LAST'
      WHEN (0 /* ASC */, 2 /* NULLS FIRST */)
        THEN 'NULLS FIRST'
      ELSE
        -- ASC NULLS LAST is default
        NULL
    END AS sort_order
  FROM
    pg_attribute
  JOIN
    pg_index
      ON indexrelid = attrelid
  JOIN
    pg_opclass
      ON pg_opclass.oid = pg_index.indclass[attnum - 1]
  LEFT JOIN
    pg_collation
      ON pg_collation.oid = pg_index.indcollation[attnum - 1]
  ORDER BY
    attnum
),
ind_col_agg AS (
  -- Format and aggregate index column components
  SELECT
    attrelid,
    array_agg(
      -- Escape double-pipe as a pipe is a table delimiter in asciidoc
        attname ||
      coalesce(
        ' ' || opcname,
        ''
      ) ||
      coalesce(
        ' COLLATE ' ||
        quote_ident(
          collname
        ),
        ''
      ) ||
      coalesce(
        ' ' || sort_order,
        ''
      )
    ) AS colldefs,
    array_agg(
      attname
    ) AS collnames
  FROM
    ind_col_data
  GROUP BY
    attrelid
),
dup_expr AS (
  SELECT
    indrelid,
    indexrelid,
    indpred,
    indcollation,
    indisunique,
    indisprimary
  FROM
    pg_index AS i
  WHERE
    EXISTS (
      SELECT
        1
      FROM
        pg_index AS i2
      WHERE
        i.indrelid = i2.indrelid
      AND
        i.indkey = i2.indkey
      AND
        coalesce(
          pg_get_expr(
            i.indexprs,
            i.indrelid
          ),
          ''
        ) =
        coalesce(
          pg_get_expr(
            i2.indexprs,
            i2.indrelid
          ),
          ''
        )
      AND
        i.indexrelid <> i2.indexrelid
    )
)
SELECT
  uind.schemaname      AS "Schema",
  uind.relname         AS "Table",
  uind.indexrelname    AS "Index",
  CASE indisprimary
    WHEN true
      THEN 'Y'
    ELSE ''
  END                  AS "PK",
  CASE indisunique
    WHEN true
      THEN 'Y'
    ELSE ''
  END                  AS "UQ",
  -- This associates indexes together by the columns they index
  row_number() OVER w  AS "#",
  array_to_string(
    colldefs,
    ', '
  )                    AS "Columns",
  pg_get_expr(
    indpred,
    indrelid,
    true
  )                    AS "Condition",
  amname               AS "AM",
  pg_size_pretty(
    pg_relation_size(
      uind.indexrelid
    )
  )                    AS "Size",
  idx_scan             AS "Scans"
FROM
  pg_stat_user_indexes AS uind
LEFT JOIN
  ind_col_agg
    ON
      ind_col_agg.attrelid = uind.indexrelid
LEFT JOIN
  dup_expr
    ON
      uind.indexrelid = dup_expr.indexrelid
JOIN
  pg_indexes
    ON
      uind.schemaname = pg_indexes.schemaname
    AND
      uind.indexrelname = pg_indexes.indexname
LEFT JOIN
  pg_class
    ON
      dup_expr.indexrelid = pg_class.oid
JOIN
  pg_am
    ON
      pg_class.relam = pg_am.oid
WINDOW w AS (
  PARTITION BY
    uind.schemaname,
    uind.relname,
    array_to_string(
      collnames,
      ', '
    )
  ORDER BY
    pg_get_expr(
      indpred,
      indrelid,
      true
    ),
    pg_relation_size(
      uind.indexrelid
    ) DESC
)
ORDER BY
  uind.schemaname,
  uind.relname,
  array_to_string(
    collnames,
    ', '
  ),
  row_number() OVER w;
