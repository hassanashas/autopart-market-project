with invoice_hdr as (
    select
        i.invoice_id,
        i.invoice_da,
        i.invtype,
        i.instanceid
    from pinnacle_interim_database.invoices i
    where i.invoice_da >= current_date - 90
),

part_lines as (
    select
        d.invoice_id,
        d.icnumber as ic,
        d.icver as icver,
        d.part,
        d.price,
        d.instanceid
    from pinnacle_interim_database.invoicedetail d
    where d.inventory_id is not null
      and d.detatiltype in ('INVENTORY_ITEM', 'BROKERED_ITEM', 'EXTRA_SALE')
),

gross_sales_90 as (
    select
        ih.instanceid,
        pl.ic,
        pl.icver,
        pl.part,
        count(*) as units_sold,
        sum(pl.price) as gross_sales,
        round(avg(pl.price))::integer as avg_gross_sale_price
    from invoice_hdr ih
    join part_lines pl
      on pl.invoice_id = ih.invoice_id
     and pl.instanceid = ih.instanceid
    where ih.invtype = 'W'
      and pl.price is not null
    group by
        ih.instanceid,
        pl.ic,
        pl.icver,
        pl.part
),
base_data as (
    select
        tq.instanceid,
        tq.ic,
        tq.part,

        coalesce(gs.units_sold, 0) as units_sold,
        coalesce(gs.gross_sales, 0) as gross_sales,
        gs.avg_gross_sale_price,

        tq.current_qoh,
        tq.target_qoh,
        tq.stock_ratio,
        tq.stock_status
    from pinnacle_interim_database.target_qoh tq
    left join gross_sales_90 gs
      on gs.instanceid = tq.instanceid
     and gs.ic = tq.ic
     and gs.part = tq.part
    where tq.part in ('AA', 'BA')
      and tq.stock_status in ('Out of Stock', 'Understocked')
      and (
            gs.avg_gross_sale_price > 750
            or gs.avg_gross_sale_price is null
          )
),
deduped_ic_part as (
    select
        ic,
        part,
        sum(units_sold) as units_sold,
        sum(gross_sales) as gross_sales,
        sum(current_qoh) as current_qoh,
        sum(target_qoh) as target_qoh,

        case
            when sum(units_sold) > 0
            then round(sum(gross_sales) / sum(units_sold))::integer
        end as avg_gross_sale_price
    from base_data
    group by
        ic,
        part
),
deduped_part AS 
(
select *
from deduped_ic_part
where
    target_qoh >= 4
    and current_qoh < target_qoh * 0.7
),
ic_canonical as (
    select
        ic,
        part,
        model,
        startyear,
        endyear,
        description
    from pinnacle_interim_database.ic
    where part in ('AA', 'BA')
    group by
        ic,
        part,
        model,
        startyear,
        endyear,
        description
),
models_canonical as (
    select
        code,
        manuname,
        name
    from pinnacle_interim_database.models
    group by
        code,
        manuname,
        name
)


select
    d.*,

    icc.model as model_code,
    mc.manuname as manufacturer,
    mc.name as model_name,
    icc.startyear,
    icc.endyear,
    icc.description as ic_description

from deduped_part d

join ic_canonical icc
  on icc.ic = d.ic
 and icc.part = d.part

left join models_canonical mc
  on mc.code = icc.model;
