create database tpch_sf100;
use tpch_sf100;
CREATE TABLE IF NOT EXISTS customer (
                                        c_custkey BIGINT not null,
                                        c_name STRING not null,
                                        c_address STRING not null,
                                        c_nationkey INTEGER not null,
                                        c_phone STRING not null,
                                        c_acctbal DECIMAL(15, 2) not null,
    c_mktsegment STRING not null,
    c_comment STRING not null
    ) CLUSTER BY (c_custkey);


CREATE TABLE IF NOT EXISTS lineitem (
                                        l_orderkey BIGINT not null,
                                        l_partkey BIGINT not null,
                                        l_suppkey BIGINT not null,
                                        l_linenumber BIGINT not null,
                                        l_quantity DECIMAL(15, 2) not null,
    l_extendedprice DECIMAL(15, 2) not null,
    l_discount DECIMAL(15, 2) not null,
    l_tax DECIMAL(15, 2) not null,
    l_returnflag STRING not null,
    l_linestatus STRING not null,
    l_shipdate DATE not null,
    l_commitdate DATE not null,
    l_receiptdate DATE not null,
    l_shipinstruct STRING not null,
    l_shipmode STRING not null,
    l_comment STRING not null
    ) CLUSTER BY(l_shipdate, l_orderkey);

CREATE TABLE IF NOT EXISTS nation (
                                      n_nationkey INTEGER not null,
                                      n_name STRING not null,
                                      n_regionkey INTEGER not null,
                                      n_comment STRING
) CLUSTER BY (n_nationkey);

CREATE TABLE IF NOT EXISTS orders (
                                      o_orderkey BIGINT not null,
                                      o_custkey BIGINT not null,
                                      o_orderstatus STRING not null,
                                      o_totalprice DECIMAL(15, 2) not null,
    o_orderdate DATE not null,
    o_orderpriority STRING not null,
    o_clerk STRING not null,
    o_shippriority INTEGER not null,
    o_comment STRING not null
    ) CLUSTER BY (o_orderkey, o_orderdate);

CREATE TABLE IF NOT EXISTS partsupp (
                                        ps_partkey BIGINT not null,
                                        ps_suppkey BIGINT not null,
                                        ps_availqty BIGINT not null,
                                        ps_supplycost DECIMAL(15, 2) not null,
    ps_comment STRING not null
    ) CLUSTER BY (ps_partkey);

CREATE TABLE IF NOT EXISTS part (
                                    p_partkey BIGINT not null,
                                    p_name STRING not null,
                                    p_mfgr STRING not null,
                                    p_brand STRING not null,
                                    p_type STRING not null,
                                    p_size INTEGER not null,
                                    p_container STRING not null,
                                    p_retailprice DECIMAL(15, 2) not null,
    p_comment STRING not null
    ) CLUSTER BY (p_partkey);

CREATE TABLE IF NOT EXISTS region (
                                      r_regionkey INTEGER not null,
                                      r_name STRING not null,
                                      r_comment STRING
) CLUSTER BY (r_regionkey);

CREATE TABLE IF NOT EXISTS supplier (
                                        s_suppkey BIGINT not null,
                                        s_name STRING not null,
                                        s_address STRING not null,
                                        s_nationkey INTEGER not null,
                                        s_phone STRING not null,
                                        s_acctbal DECIMAL(15, 2) not null,
    s_comment STRING not null
    ) CLUSTER BY (s_suppkey);


copy into lineitem from 's3://redshift-downloads/TPC-H/2.18/100GB/lineitem/' CONNECTION = (allow_anonymous='true' region='us-east-1')  file_format=(TYPE=CSV, field_delimiter='|');
copy into nation from 's3://redshift-downloads/TPC-H/2.18/100GB/nation/' CONNECTION = (allow_anonymous='true' region='us-east-1')  file_format=(TYPE=CSV, field_delimiter='|');
copy into orders from 's3://redshift-downloads/TPC-H/2.18/100GB/orders/' CONNECTION = (allow_anonymous='true' region='us-east-1')  file_format=(TYPE=CSV, field_delimiter='|');
copy into part from 's3://redshift-downloads/TPC-H/2.18/100GB/part/' CONNECTION = (allow_anonymous='true' region='us-east-1')  file_format=(TYPE=CSV, field_delimiter='|');
copy into partsupp from 's3://redshift-downloads/TPC-H/2.18/100GB/partsupp/' CONNECTION = (allow_anonymous='true' region='us-east-1')  file_format=(TYPE=CSV, field_delimiter='|');
copy into region from 's3://redshift-downloads/TPC-H/2.18/100GB/region/' CONNECTION = (allow_anonymous='true' region='us-east-1')  file_format=(TYPE=CSV, field_delimiter='|');
copy into supplier from 's3://redshift-downloads/TPC-H/2.18/100GB/supplier/' CONNECTION = (allow_anonymous='true' region='us-east-1')  file_format=(TYPE=CSV, field_delimiter='|');