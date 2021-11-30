module QueryBuilder
  class Keywords
    COLUMN_DEPENDENCIES = {
        keyword_name:     'keywords',
        location_name:    'locations',
        search_volume:    'adwords',
        average_cpc:      'adwords',
        competition:      'adwords',
        impressions:      'gwt',
        clicks:           'gwt',
        average_position: 'gwt',
        ctr:              'gwt',
        rank:             'ranks',
        rank_change:      'rank_changes',
        projected_search_volume: 'ranks',
        visits:           'analytics',
        conversions:      'analytics',
        revenue:          'analytics',
    }

    STEP_DEPENDENCIES = {
        'site_keyword_variants'   => %w(site_keywords),
        'keywords'                => %w(site_keywords),
        'locations'               => %w(site_keyword_variants),
        'adwords'                 => %w(site_keywords),
        'gwt'                     => %w(keywords),
        'analytics'               => %w(site_keywords),
        'ranks'                   => %w(site_keywords site_keyword_variants),
        'rank_changes'            => %w(site_keywords site_keyword_variants),
        'serp_properties'         => %w(site_keywords site_keyword_variants),
    }

    class << self
      # Builds optimized SELECT queries using CTE.
      # CTE stands for Common Table Expressions:
      # https://www.postgresql.org/docs/9.6/static/queries-with.html
      def select(columns, opts)
        source_site = opts[:site].source_site || opts[:site]
        sites = [source_site] + Widgets::Widget.competitor_sites(opts.merge(site: source_site))
        search_engines = source_site.search_engines

        cte_steps = cte_steps(opts)

        cte_queries = []
        selected_columns = []

        cte_steps.each_with_index do |step, i|
          base_table = i > 0 ? "CTE#{i-1}" : nil

          selects, conditions, joins, grouping, base_table = cte_query_chunks(step, opts, sites, search_engines, base_table)
          selects.each{ |s| selected_columns << s.split(/[\s\.]/).last }

          selects.unshift("CTE#{i-1}.*")  if i > 0

          last_step = (i + 1 == cte_steps.count)
          query = "
              SELECT
                  #{selects.join(', ')}
                  #{last_step ? ', COUNT(*) OVER() AS total_count' : ''}
              FROM
                  #{base_table}
                  #{joins.uniq.join(' ')}
              #{conditions.present? ? "WHERE #{conditions.join(' AND ')}" : ''}
              #{grouping}
              #{last_step ?
                  "ORDER BY #{opts[:sort_col]} #{opts[:sort_dir]} NULLS LAST
                  LIMIT :limit OFFSET :offset" : ''}"

          cte_queries << "CTE#{i} AS (#{query})"
        end

        base_table = "CTE#{cte_queries.count-1}"
        selects, conditions, joins = missing_columns_query_chunks(columns, selected_columns, opts, sites, search_engines, base_table)

        query = "
            WITH #{cte_queries.join(', ')}
            SELECT
                #{base_table}.*,
                #{selects.join(', ')}
            FROM
                #{base_table}
                #{joins.uniq.join(' ')}
            #{conditions.present? ? "WHERE #{conditions.join(' AND ')}" : ''}
            ORDER BY #{opts[:sort_col]} #{opts[:sort_dir]} NULLS LAST"

        (query =~ /:\w+/) ? Core::Models::Base.send(:sanitize_sql_array, [query, opts]) : query
      end

      private

      def cte_steps(opts)
        sort_step = sort_step(opts[:sort_col])

        filter_steps = %w(site_keywords site_keyword_variants)

        if opts[:search_string].present?
          filter_steps << 'keywords'  unless sort_step == 'keywords'
        end

        if opts[:bucket].present?
          filter_steps << 'ranks'  unless sort_step == 'ranks'
        end

        if opts[:serp_property_ids].present?
          filter_steps << 'serp_properties'  unless sort_step == 'serp_properties'
        end

        steps = filter_steps + [sort_step]

        add_step_dependencies(steps)
      end

      def sort_step(sort_col)
        case sort_col
          when /^site_[\d]+_rank$/, /^site_[\d]+_projected_search_volume$/
            step = "#{COLUMN_DEPENDENCIES[:rank]}:site_#{sort_col.split('_')[1]}"

          when /^se_[\d]+_rank$/, /^se_[\d]+_projected_search_volume$/
            step = "#{COLUMN_DEPENDENCIES[:rank]}:se_#{sort_col.split('_')[1]}"

          when /^site_[\d]+_rank_change$/
            step = "#{COLUMN_DEPENDENCIES[:rank_change]}:site_#{sort_col.split('_')[1]}"

          when /^se_[\d]+_rank_change$/
            step = "#{COLUMN_DEPENDENCIES[:rank_change]}:se_#{sort_col.split('_')[1]}"

          else
            step = COLUMN_DEPENDENCIES[sort_col.to_sym]
            raise "Unknown sorting for widget: #{sort_col}" unless step
        end

        step
      end

      def add_step_dependencies(steps)
        last = steps.pop
        if last
          resolved = add_step_dependencies(steps)
          if STEP_DEPENDENCIES[last]
            STEP_DEPENDENCIES[last].each do |dep|
              resolved << dep  unless resolved.include?(dep)
            end
          end

          resolved << last  unless resolved.include?(last)
        else
          []
        end
      end

      def cte_query_chunks(step, opts, sites, search_engines, base_table)
        selects = []
        conditions = []
        joins = []
        grouping = ''

        case step.split(':').first
          when 'site_keywords'
            base_table = "#{Core::Models::SiteKeyword.table_name} _SK"
            selects <<
                '_SK.id AS site_keyword_id' <<
                '_SK.keyword_id'
            conditions << "_SK.site_id = #{sites[0].id}"

            if opts[:page_id].present?
              selects << "_PK.page_id"
              joins <<
                  "JOIN #{Core::Models::PageKeyword.table_name} _PK
                      ON _PK.keyword_id = _SK.keyword_id AND _PK.page_id = :page_id"
              conditions << "_PK.site_id = #{opts[:site].id}"
            end

            if opts[:keyword_tag_ids].present?
              sub_conditions = ['_KTSK.keyword_tag_id IN (:keyword_tag_ids)']
              if opts[:keyword_tag_ids].include?(0) || opts[:keyword_tag_ids].include?('0')
                sub_conditions << '_KTSK.keyword_tag_id IS NULL'
              end
              conditions << "(#{sub_conditions.join(' OR ')})"
              joins <<
                  'LEFT JOIN core.keyword_tags_site_keywords _KTSK ON _KTSK.site_keyword_id = _SK.id'
              grouping = 'GROUP BY _SK.id'
            end

          when 'site_keyword_variants'
            selects <<
                '_SKV.location_id' <<
                '_SKV.mysql_keyword_id' <<
                '_SKV.discovered_competitor_blacklisted' <<
                '_SKV.id AS site_keyword_variant_id'
            joins <<
                "JOIN #{Core::Models::SiteKeywordVariant.table_name} _SKV ON _SKV.site_keyword_id = #{base_table}.site_keyword_id"

            if opts[:location_ids].present?
              if opts[:location_ids].first == Geonames::Accessors::Name::ALL_LOCATIONS_BUT_GLOBAL
                conditions << '_SKV.location_id > 0'
              else
                conditions << '_SKV.location_id IN (:location_ids)'
              end
            end

          when 'keywords'
            selects <<
                '_K.name AS keyword_name'
            joins <<
                "JOIN #{Core::Models::Keyword.table_name} _K ON _K.id = #{base_table}.keyword_id"

            if opts[:search_string].present?
              if opts[:search_string].starts_with?("\"") && opts[:search_string].ends_with?("\"")
                conditions << 'lower(_K.name) = :search_string_sql'
                opts[:search_string_sql] = opts[:search_string][1...-1].downcase
              else
                conditions << 'lower(_K.name) LIKE :search_string_sql'
                opts[:search_string_sql] = "%#{opts[:search_string]}%".downcase
              end
            end

          when 'locations'
            selects <<
                "concat_ws(', ', _GN.name, _GN.admin1_code) AS location_name"
            joins <<
                "LEFT JOIN #{Geonames::Models::Name.table_name} _GN ON _GN.name_id = #{base_table}.location_id"

          when 'adwords'
            selects <<
                '_KS.search_volume' <<
                '_KS.average_cpc' <<
                '_KS.competition'

            country_code = sites[0].geo.try(:country_code)
            if country_code.present?
              opts[:location_code] = country_code
              opts[:location_code] = 'gb' if opts[:location_code] == 'uk'
              joins <<
                  "LEFT JOIN (
                      SELECT __KS.*
                      FROM #{Adwords::Models::KeywordStatistics.table_name} __KS
                      JOIN #{Adwords::Models::Location.table_name} __L
                          ON __KS.location_criteria_id = __L.criteria_id AND __L.code = :location_code
                      ) _KS ON _KS.keyword_id = #{base_table}.keyword_id AND _KS.date = :to_bom"
            else
              joins <<
                  "LEFT JOIN #{Adwords::Models::KeywordStatistics.table_name} _KS
                      ON _KS.keyword_id = #{base_table}.keyword_id AND _KS.date = :to_bom AND _KS.location_criteria_id IS NULL"
            end

          when 'gwt'
            selects <<
                '_SKSM.impressions' <<
                '_SKSM.clicks' <<
                '(_SKSM.weighted_average_position / NULLIF(_SKSM.impressions, 0)) AS average_position' <<
                '(_SKSM.weighted_ctr / NULLIF(_SKSM.impressions, 0)) AS ctr'
            joins <<
                "LEFT JOIN (
                    SELECT
                        keyword,
                        SUM(impressions) AS impressions,
                        SUM(clicks) AS clicks,
                        SUM(average_position * impressions) AS weighted_average_position,
                        SUM(ctr * impressions) AS weighted_ctr
                    FROM #{GWT::Models::SiteKeywordSearchMetric.table_name}
                    WHERE site_id = #{opts[:site].id} AND date BETWEEN :from AND :to
                    GROUP BY keyword) _SKSM
                    ON _SKSM.keyword = #{base_table}.keyword_name"

          when 'analytics'
            selects <<
                '_SOV.page_views' <<
                '_SOV.visits' <<
                '_SOV.visitors' <<
                '_SOV.new_visits' <<
                '_SOV.bounces' <<
                '_SOV.conversions' <<
                '_SOV.value AS revenue' <<
                '_SOV.session_duration'

            sub_conditions = ["site_id = #{opts[:site].id}", 'date BETWEEN :from AND :to']
            if opts[:search_engine].present?
              sub_conditions << "search_engine_id = #{opts[:search_engine].id}"
            end
            if opts[:device_ids].present?
              sub_conditions << 'device_id IN (:device_ids)'
            end

            joins <<
                "LEFT JOIN (
                    SELECT
                        keyword_id,
                        SUM(page_views)       AS page_views,
                        SUM(visits)           AS visits,
                        SUM(visitors)         AS visitors,
                        SUM(new_visits)       AS new_visits,
                        SUM(bounces)          AS bounces,
                        SUM(conversions)      AS conversions,
                        SUM(value)            AS value,
                        SUM(session_duration) AS session_duration
                    FROM #{Analytics::Models::SiteOrganicVisit.table_name}
                    WHERE #{sub_conditions.join(' AND ')}
                    GROUP BY keyword_id) _SOV
                    ON _SOV.keyword_id = #{base_table}.keyword_id"

          when 'ranks'
            site_id = opts[:site].id
            search_engine_id = opts[:search_engine].try(:id)
            prefix  = ''
            table   = '_R'

            if step[':site_']
              i = step.split(':site_').last.to_i
              site_id = sites[i].id
              prefix  = "site_#{i}_"
              table   = "_R_#{i}"

            elsif step[':se_']
              i = step.split(':se_').last.to_i
              search_engine_id = search_engines[i].id
              prefix  = "se_#{i}_"
              table   = "_R_#{i}"
            end

            selects <<
                "#{table}.rank AS #{prefix}rank" <<
                "#{table}.projected_search_volume AS #{prefix}projected_search_volume" <<
                "#{table}.page_id AS #{prefix}page_id"

            joins <<
                "LEFT JOIN #{Ranking::Models::SiteKeywordBestRank.table_name} #{table} ON
                    #{table}.keyword_id = #{base_table}.keyword_id AND
                    #{table}.location_id = #{base_table}.location_id AND
                    #{opts[:page_id].present? ? "#{table}.page_id = #{base_table}.page_id AND" : ''}
                    #{table}.site_id = #{site_id} AND
                    #{table}.search_engine_id = #{search_engine_id} AND
                    #{table}.date = :to"

            if opts[:bucket].present?
              bucket_conds = Ranking::Accessors::Rankings::BUCKETS.values
              if bucket_conds[opts[:bucket].to_i]
                conditions << bucket_conds[opts[:bucket].to_i].sub('WHEN', '(').sub('THEN', ')').gsub('rank', "#{table}.rank")
              end
            end

          when 'rank_changes'
            site_id = opts[:site].id
            search_engine_id = opts[:search_engine].try(:id)
            prefix  = ''
            table   = '_R'
            table2  = '_R2'

            if step[':site_']
              i = step.split(':site_').last.to_i
              site_id = sites[i].id
              prefix  = "site_#{i}_"
              table   = "_R_#{i}"
              table2  = "_R2_#{i}"

            elsif step[':se_']
              i = step.split(':se_').last.to_i
              search_engine_id = search_engines[i].id
              prefix  = "se_#{i}_"
              table   = "_R_#{i}"
              table2  = "_R2_#{i}"
            end

            selects <<
                "#{table}.rank AS #{prefix}rank" <<
                "LEAST(#{table2}.rank, 51) - LEAST(#{table}.rank, 51) AS #{prefix}rank_change" <<
                "#{table}.projected_search_volume AS #{prefix}projected_search_volume" <<
                "#{table}.page_id AS #{prefix}page_id"

            joins <<
                "LEFT JOIN #{Ranking::Models::SiteKeywordBestRank.table_name} #{table} ON
                    #{table}.keyword_id = #{base_table}.keyword_id AND
                    #{table}.location_id = #{base_table}.location_id AND
                    #{opts[:page_id].present? ? "#{table}.page_id = #{base_table}.page_id AND" : ''}
                    #{table}.site_id = #{site_id} AND
                    #{table}.search_engine_id = #{search_engine_id} AND
                    #{table}.date = :to" <<
                "LEFT JOIN #{Ranking::Models::SiteKeywordBestRank.table_name} #{table2} ON
                    #{table2}.keyword_id = #{base_table}.keyword_id AND
                    #{table2}.location_id = #{base_table}.location_id AND
                    #{opts[:page_id].present? ? "#{table}.page_id = #{base_table}.page_id AND" : ''}
                    #{table2}.site_id = #{site_id} AND
                    #{table2}.search_engine_id = #{search_engine_id} AND
                    #{table2}.date = :from"

          when 'serp_properties'
            selects << '_PSP.properties AS serp_properties'

            joins <<
                "LEFT JOIN #{Ranking::Models::ParsedSerpProperty.table_name} _PSP ON
                    _PSP.keyword_id = #{base_table}.keyword_id AND
                    _PSP.location_id = #{base_table}.location_id AND
                    _PSP.search_engine_id = #{opts[:search_engine].id} AND
                    _PSP.date = :to"

            if opts[:serp_property_ids].present?
              conditions << "(#{opts[:serp_property_ids].map{ |property_id| "_PSP.properties & #{0b111 << 3 * (property_id.to_i - 1)} <> 0" }.join(' OR ')})"
            end

          else
            raise "Unknown step: #{step}"
        end

        [selects, conditions, joins, grouping, base_table]
      end

      def missing_columns_query_chunks(columns, selected_columns, opts, sites, search_engines, base_table)
        selects = []
        conditions = []
        joins = {}

        missing_columns = columns - selected_columns
        missing_columns.each do |column|
          case column
            when 'keyword_name'
              selects <<
                  'K.name AS keyword_name'
              joins['K'] =
                  "JOIN #{Core::Models::Keyword.table_name} K ON K.id = #{base_table}.keyword_id"

            when 'mysql_keyword_id', 'location_id'
              selects <<
                  'SKV.location_id' <<
                  'SKV.mysql_keyword_id' <<
                  'SKV.discovered_competitor_blacklisted' <<
                  'SKV.id AS site_keyword_variant_id'
              joins['SKV'] =
                  "JOIN #{Core::Models::SiteKeywordVariant.table_name} SKV ON SKV.site_keyword_id = #{base_table}.site_keyword_id"

            when 'location_name'
              selects <<
                  "concat_ws(', ', GN.name, GN.admin1_code) AS location_name"
              joins['GN'] =
                  "LEFT JOIN #{Geonames::Models::Name.table_name} GN
                      ON GN.name_id = #{selected_columns.include?('location_id') ? base_table : 'SKV'}.location_id"

            when 'search_volume', 'average_cpc', 'competition'
              selects <<
                  'KS.search_volume' <<
                  'KS.average_cpc' <<
                  'KS.competition'

              country_code = sites[0].geo.try(:country_code)
              if country_code.present?
                opts[:location_code] = country_code
                opts[:location_code] = 'gb' if opts[:location_code] == 'uk'
                joins['KS'] =
                    "LEFT JOIN (
                        SELECT _KS.*
                        FROM #{Adwords::Models::KeywordStatistics.table_name} _KS
                        JOIN #{Adwords::Models::Location.table_name} _L
                            ON _KS.location_criteria_id = _L.criteria_id AND _L.code = :location_code
                        ) KS ON KS.keyword_id = #{base_table}.keyword_id AND KS.date = :to_bom"
              else
                joins['KS'] =
                    "LEFT JOIN #{Adwords::Models::KeywordStatistics.table_name} KS
                        ON KS.keyword_id = #{base_table}.keyword_id AND KS.date = :to_bom AND KS.location_criteria_id IS NULL"
              end

            when 'impressions', 'clicks', 'average_position', 'ctr'
              selects <<
                  'SKSM.impressions' <<
                  'SKSM.clicks' <<
                  '(SKSM.weighted_average_position / NULLIF(SKSM.impressions, 0)) AS average_position' <<
                  '(SKSM.weighted_ctr / NULLIF(SKSM.impressions, 0)) AS ctr'

              joins['SKSM'] =
                  "LEFT JOIN (
                      SELECT
                          keyword,
                          SUM(impressions) AS impressions,
                          SUM(clicks) AS clicks,
                          SUM(average_position * impressions) AS weighted_average_position,
                          SUM(ctr * impressions) AS weighted_ctr
                      FROM #{GWT::Models::SiteKeywordSearchMetric.table_name}
                      WHERE site_id = #{opts[:site].id} AND date BETWEEN :from AND :to
                      GROUP BY keyword) SKSM
                      ON SKSM.keyword = #{selected_columns.include?('keyword_name') ? "#{base_table}.keyword_name" : 'K.name'}"

            when 'visits', 'conversions', 'revenue'
              selects <<
                  'SOV.page_views' <<
                  'SOV.visits' <<
                  'SOV.visitors' <<
                  'SOV.new_visits' <<
                  'SOV.bounces' <<
                  'SOV.conversions' <<
                  'SOV.value AS revenue' <<
                  'SOV.session_duration'

              sub_conditions = ["site_id = #{opts[:site].id}", 'date BETWEEN :from AND :to']
              if opts[:search_engine].present?
                sub_conditions << "search_engine_id = #{opts[:search_engine].id}"
              end
              if opts[:device_ids].present?
                sub_conditions << 'device_id IN (:device_ids)'
              end

              joins['SOV'] =
                  "LEFT JOIN (
                      SELECT
                          keyword_id,
                          SUM(page_views)       AS page_views,
                          SUM(visits)           AS visits,
                          SUM(visitors)         AS visitors,
                          SUM(new_visits)       AS new_visits,
                          SUM(bounces)          AS bounces,
                          SUM(conversions)      AS conversions,
                          SUM(value)            AS value,
                          SUM(session_duration) AS session_duration
                      FROM #{Analytics::Models::SiteOrganicVisit.table_name}
                      WHERE #{sub_conditions.join(' AND ')}
                      GROUP BY keyword_id) SOV
                      ON SOV.keyword_id = #{base_table}.keyword_id"

            when 'rank', 'projected_search_volume', 'page_id', /^site_[\d]+_rank$/, /^site_[\d]+_projected_search_volume/, /^site_[\d]+_page_id/, /^se_[\d]+_rank$/, /^se_[\d]+_projected_search_volume/, /^se_[\d]+_page_id/
              site_id = opts[:site].id
              search_engine_id = opts[:search_engine].try(:id)
              prefix  = ''
              table   = 'R'

              if column['site_']
                i = column.split('_')[1].to_i
                site_id = sites[i].id
                prefix  = "site_#{i}_"
                table   = "R_#{i}"

              elsif column['se_']
                i = column.split('_')[1].to_i
                search_engine_id = search_engines[i].id
                prefix  = "se_#{i}_"
                table   = "R_#{i}"
              end

              selects <<
                  "#{table}.rank AS #{prefix}rank" <<
                  "#{table}.projected_search_volume AS #{prefix}projected_search_volume" <<
                  "#{table}.page_id AS #{prefix}page_id"

              joins[table] =
                  "LEFT JOIN #{Ranking::Models::SiteKeywordBestRank.table_name} #{table} ON
                      #{table}.keyword_id = #{base_table}.keyword_id AND
                      #{table}.location_id = #{selected_columns.include?('location_id') ? base_table : 'SKV'}.location_id AND
                      #{table}.site_id = #{site_id} AND
                      #{table}.search_engine_id = #{search_engine_id} AND
                      #{table}.date = :to"

            when 'rank_change', /^site_[\d]+_rank_change$/, /^se_[\d]+_rank_change$/
              i = 0
              site_id = opts[:site].id
              search_engine_id = opts[:search_engine].try(:id)
              prefix  = ''
              table   = 'R'
              table2  = 'R2'

              if column['site_']
                i = column.split('_')[1].to_i
                site_id = sites[i].id
                prefix  = "site_#{i}_"
                table   = "R_#{i}"
                table2  = "R2_#{i}"

              elsif column['se_']
                i = column.split('_')[1].to_i
                search_engine_id = search_engines[i].id
                prefix  = "se_#{i}_"
                table   = "R_#{i}"
                table2  = "R2_#{i}"
              end

              if selected_columns.include?("#{prefix}rank")
                selects <<
                    "LEAST(#{table2}.rank, 51) - LEAST(#{base_table}.#{prefix}rank, 51) AS #{prefix}rank_change"
              else
                selects <<
                    "LEAST(#{table2}.rank, 51) - LEAST(#{table}.rank, 51) AS #{prefix}rank_change"

                joins[table] =
                    "LEFT JOIN #{Ranking::Models::SiteKeywordBestRank.table_name} #{table} ON
                        #{table}.keyword_id = #{base_table}.keyword_id AND
                        #{table}.location_id = #{selected_columns.include?('location_id') ? base_table : 'SKV'}.location_id AND
                        #{table}.site_id = #{site_id} AND
                        #{table}.search_engine_id = #{search_engine_id} AND
                        #{table}.date = :to"
              end

              joins[table2] =
                  "LEFT JOIN #{Ranking::Models::SiteKeywordBestRank.table_name} #{table2} ON
                      #{table2}.keyword_id = #{base_table}.keyword_id AND
                      #{table2}.location_id = #{selected_columns.include?('location_id') ? base_table : 'SKV'}.location_id AND
                      #{table2}.site_id = #{site_id} AND
                      #{table2}.search_engine_id = #{search_engine_id} AND
                      #{table2}.date = :from"

            when 'local_rank', 'nonlocal_rank'
              selects <<
                  'RM.local_rank' <<
                  'RM.nonlocal_rank'

              joins['RM'] =
                  "LEFT JOIN #{Ranking::Models::SiteKeywordBestMultirank.table_name} RM ON
                      RM.keyword_id = #{base_table}.keyword_id AND
                      RM.location_id = #{selected_columns.include?('location_id') ? base_table : 'SKV'}.location_id AND
                      RM.site_id = #{opts[:site].id} AND
                      RM.search_engine_id = #{opts[:search_engine].id} AND
                      RM.date = :to"

            when 'page_title', 'page_url'
              selects <<
                  'P.url AS page_url' <<
                  'PD.title AS page_title'

              if selected_columns.include?('page_id')
                joins['P'] =
                    "LEFT JOIN #{Core::Models::Page.table_name} P ON
                        P.site_id = #{opts[:site].id} AND
                        P.id = #{base_table}.page_id"
              else
                joins['P'] =
                    "LEFT JOIN #{Core::Models::Page.table_name} P ON
                        P.site_id = #{opts[:site].id} AND
                        P.id = R.page_id"
                joins['R'] =
                    "LEFT JOIN #{Ranking::Models::SiteKeywordBestRank.table_name} R ON
                        R.keyword_id = #{base_table}.keyword_id AND
                        R.location_id = #{selected_columns.include?('location_id') ? base_table : 'SKV'}.location_id AND
                        R.site_id = #{opts[:site].id} AND
                        R.search_engine_id = #{opts[:search_engine].id} AND
                        R.date = :to"
              end
              joins['PD'] =
                  "LEFT JOIN #{Core::Models::PageDescription.table_name} PD ON
                      PD.page_id = P.id"

            when 'serp_properties'
              selects << 'PSP.properties AS serp_properties'

              joins['PSP'] =
                  "LEFT JOIN #{Ranking::Models::ParsedSerpProperty.table_name} PSP ON
                      PSP.keyword_id = #{base_table}.keyword_id AND
                      PSP.location_id = #{base_table}.location_id AND
                      PSP.search_engine_id = #{opts[:search_engine].id} AND
                      PSP.date = :to"

            else
              raise "Unknown column: #{column}"
          end
        end

        [selects.uniq, conditions, joins.values]
      end
    end
  end
end
