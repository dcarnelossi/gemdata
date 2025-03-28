
def globalsqlscriptsmeta(schema):
    scripts = f"""
           DROP TABLE IF EXISTS "{schema}".orders_ia_meta;

            CREATE TABLE "{schema}".orders_ia_meta AS


            -- Parte 1: CTE para cálculo
            WITH fatdiario AS (
            select 
            DATE_TRUNC('day',  creationdate)   as dategenerate,
            cast(round(cast(SUM(revenue) as numeric),2) as float) as faturamento
            from  "{schema}".orders_ia ia 
            group by 
            1                             
            ),
            daily_real as (
                SELECT 
                    DATE_TRUNC('day', dategenerate) AS day,
                    EXTRACT(DOW FROM dategenerate) AS weekday,  -- 0=Domingo, 6=Sábado
                    SUM(faturamento) AS total_daily_revenue
                FROM fatdiario
                GROUP BY DATE_TRUNC('day', dategenerate), EXTRACT(DOW FROM dategenerate)
            ),
            weekly_weights AS (
                SELECT 
                    weekday,
                    SUM(total_daily_revenue) AS total_revenue_weekday,
                    SUM(SUM(total_daily_revenue)) OVER () AS total_revenue_all
                FROM daily_real
                GROUP BY weekday
            ),
            final_weights AS (
                SELECT 
                    weekday,
                    total_revenue_weekday,
                    total_revenue_weekday / total_revenue_all AS weight
                FROM weekly_weights
            )
            ,monthly_data AS (
                SELECT 
                    year,
                    month,
                    goal AS predicted_revenue,
                    DATE_TRUNC('month', (TO_DATE(year || '-' || month, 'YYYY-MM') + TIME '03:00:00') AT TIME ZONE 'UTC' ) AS start_date,
                    (DATE_TRUNC('month', (TO_DATE(year || '-' || month, 'YYYY-MM') + TIME '03:00:00') AT TIME ZONE 'UTC') 
                    + INTERVAL '1 month' - INTERVAL '1 day') AS end_date
                FROM "{schema}".stg_teamgoal
            ),
            daily_distribution AS (
                SELECT 
                    md.year,
                    md.month,
                    g.date AT TIME ZONE 'UTC' AS day,
                    EXTRACT(DOW FROM g.date) AS weekday,
                    predicted_revenue , -- Dia da semana,
                    COUNT(1) OVER (
                    PARTITION BY md.year, md.month, EXTRACT(DOW FROM g.date)
                ) AS weekday_count
                FROM monthly_data md
                CROSS JOIN GENERATE_SERIES(md.start_date, md.end_date, INTERVAL '1 day') AS g(date)
                ORDER BY md.year, md.month, g.date
            )
            ,
            final_distribution AS (
                SELECT 
                    dd.day,
                    dd.year,
                    dd.month,
                    dd.predicted_revenue,
                    fw.weight,
                    round(cast((dd.predicted_revenue * fw.weight)/weekday_count as numeric),2) AS daily_revenue
                FROM daily_distribution dd
                JOIN final_weights fw
                ON dd.weekday = fw.weekday
            )
            select * from final_distribution;



    """
    # print(scripts)
    return scripts




# if __name__ == "__main__":
#     with open("Output.txt", "w") as text_file:
#         text_file.write(vtexsqlscriptsorderslistupdate("6d41d249-d875-41ef-800e-eb0941f6d86f"))
#         print(vtexsqlscriptsorderslistupdate("6d41d249-d875-41ef-800e-eb0941f6d86f"))