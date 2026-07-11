-- Universe tier views
-- -----------------------------------------------------------------------------
-- Derived from universe.n750. The sync writes only n750; these views stay current
-- automatically based on the tier tag stored in n750.indices.

create schema if not exists universe;

drop view if exists universe.nifty;
create view universe.nifty as
select ticker, company_name, sector, industry, indices, updated_at
from universe.n750
where 'NIFTY50' = any(indices);

drop view if exists universe.nifty50;
create view universe.nifty50 as
select ticker, company_name, sector, industry, indices, updated_at
from universe.n750
where 'NIFTY50' = any(indices);

drop view if exists universe.niftynext50;
create view universe.niftynext50 as
select ticker, company_name, sector, industry, indices, updated_at
from universe.n750
where 'NIFTYNEXT50' = any(indices);

drop view if exists universe.midcap150;
create view universe.midcap150 as
select ticker, company_name, sector, industry, indices, updated_at
from universe.n750
where 'MIDCAP150' = any(indices);

drop view if exists universe.smallcap250;
create view universe.smallcap250 as
select ticker, company_name, sector, industry, indices, updated_at
from universe.n750
where 'SMALLCAP250' = any(indices);

drop view if exists universe.microcap250;
create view universe.microcap250 as
select ticker, company_name, sector, industry, indices, updated_at
from universe.n750
where 'MICROCAP250' = any(indices);
