-- author_ = panying
-- 模块目的：对线上消费者情况进行全景扫描
-- 最后编辑时间："2017-10-17"

-- 全景扫描-外部市场-品牌======================================
USE data_check;
DROP TABLE
IF EXISTS mkt_scan_brand;
CREATE TABLE mkt_scan_brand AS
SELECT
 tt.year_mon,
 tt.channel_name,
 tt.brand_name,
 tt.chan_bran_num,
 tt.chan_bran_sale,
 tt.price_per,
 tl.industry_sale
FROM
(SELECT
 TRIM(CONCAT(LEFT(tt.年月,4),"-",RIGHT(tt.年月,2),"-01")) AS year_mon,
 tt.`渠道` AS channel_name,
 tt.品牌名称 AS brand_name,
 SUM(tt.`销售额`) AS chan_bran_sale,
 SUM(tt.`销量`) AS chan_bran_num,
 ROUND(SUM(tt.`销售额`)/SUM(tt.`销量`),1) AS price_per
FROM
master_data.monitor_industry_data tt
GROUP BY
 year_mon,
 channel_name,
 brand_name
HAVING
 ROUND(LEFT(year_mon,4),0)>2015
ORDER BY
 year_mon DESC,
 channel_name,
 chan_bran_sale)tt
LEFT JOIN
(SELECT -- 保健食品行业销售额
 TRIM(CONCAT(LEFT(tt.年月,4),"-",RIGHT(tt.年月,2),"-01")) AS year_mon,
 tt.`渠道` AS channel_name,
 tt.品牌名称 AS brand_name,
 tt.销售额 AS brand_sale,
 ROUND(LEFT(tt.销售额行业占比,4),2) AS brand_sale_ratio,
 ROUND(100*ROUND(tt.销售额)/ROUND(LEFT(tt.销售额行业占比,4),2),1) AS industry_sale
 FROM
 master_data.monitor_industry_data tt
 WHERE
 tt.品牌名称="BY－HEALTH/汤臣倍健"
 AND
 LEFT(tt.年月,4)>2015)tl
ON CONCAT(tt.year_mon,tt.channel_name)=CONCAT(tl.year_mon,tl.channel_name);

-- 全景扫描-内部市场-======================================
-- base:全景扫描-地域分布-基础数据
USE data_check;
SET @sys_customer_index = ROUND((SELECT MAX(sys_customer_id) FROM crm_kd.kd_customer),0);
DROP TABLE 
IF EXISTS customer_data_city;
CREATE TABLE customer_data_city AS
SELECT
 tt.sys_customer_id,
 tt.out_nick,
 tt.customer_name,
 tt.mobile,
 tt.province,
 tt.city,
 tt.plat_from_type,
 tt.develop_time,
 tt.first_pay_time,
 tt.last_pay_time,
 tt.pay_times,
 tt.pay_amount
FROM
(
(SELECT
 tt.sys_customer_id,
 tt.out_nick,
 tt.customer_name,
 tt.mobile,
 tt.province,
 tt.city,
 tt.plat_from_type,
 tt.develop_time,
 tt.first_pay_time,
 tt.last_pay_time,
 tt.pay_times,
 tt.pay_amount
FROM
 crm_kd.kd_customer tt
WHERE
 DATE_FORMAT(tt.develop_time,"%Y-%m-%d") BETWEEN "2016-01-01" AND "2017-10-01"
AND
 sys_customer_id NOT IN(SELECT tt.sys_customer_id FROM crm_kd.kd_customer_property_value tt WHERE LENGTH(tt.ueInfo6)>2)
AND
 sys_customer_id NOT IN(SELECT tt.sys_customer_id FROM data_check.unnorm_customer_id tt)
ORDER BY
 tt.develop_time DESC)
UNION
(SELECT
 @sys_customer_index:= @sys_customer_index+1 AS sys_customer_id,
 tt.out_nick,
 tt.customer_name,
 tt.mobile,
 tt.province,
 tt.city,
 tt.plat_from_type,
 MIN(tt.develop_time) AS develop_time,
 MIN(tt.order_pay_time) AS first_pay_time,
 MAX(tt.order_pay_time) AS last_pay_time,
 COUNT(DISTINCT order_id) AS pay_times,
 SUM(tt.order_pay_amount) AS pay_amount
FROM
(SELECT
 tt.`用户名` AS out_nick,
 tt.`订单ID辅助列` AS order_id,
 tt.`收货人` AS customer_name,
 tt.`收货人电话` AS mobile,
 tt.`收货省份` AS province,
 tt.`收货城市` AS city,
 DATE_FORMAT(tt.`下单时间`,"%Y-%m-%d %h:%m:%s") AS develop_time,
 DATE_FORMAT(tt.`付款时间`,"%Y-%m-%d %h:%m:%s") AS order_pay_time,
 ROUND(tt.GMV,1) AS order_pay_amount,
 "46" AS plat_from_type
FROM
 master_data.mas_fristdrug_data2 tt
WHERE 
 tt.`用户名`<> 'N')tt
GROUP BY
 tt.out_nick,
 tt.customer_name,
 tt.mobile,
 tt.province,
 tt.city,
 tt.plat_from_type
ORDER BY
 pay_times DESC))tt;


-- 地域分布: 消费者渗透力
USE data_check;
DROP TABLE
IF EXISTS user_province_test;
CREATE TABLE user_province_test AS
SELECT
 pp.province as std_province,
 tt.province as tt_province,
 tt.province_counts AS province_counts,
 pp.population AS population,
 ROUND(10*tt.province_counts/pp.population,0) AS user_per_wan
FROM
(SELECT
 tt.province,
 SUM(tt.user_counts) AS province_counts
FROM
(SELECT
 TRIM(LEFT(tt.province,2)) AS province,
 COUNT(DISTINCT tt.sys_customer_id) AS user_counts
FROM
 data_check.customer_data_city tt
GROUP BY
 province
ORDER BY
 province,
 user_counts DESC) tt
GROUP BY
 tt.province)tt
LEFT JOIN data_check.province_population pp
ON LEFT(tt.province,2)=LEFT(pp.province,2)
WHERE
pp.province IS NOT NULL
ORDER BY
user_per_wan;

-- 地域分布: 区域分布
USE data_check;
DROP TABLE
IF EXISTS user_province_shop;
CREATE TABLE user_province_shop AS
SELECT
 pp.province as std_province,
 tt.province as tt_province,
 tt.province_counts AS province_counts,
 pp.shops AS shops,
 ROUND(10*tt.province_counts/pp.shops,0) AS user_per_shop
FROM
(SELECT
 tt.province,
 SUM(tt.user_counts) AS province_counts
FROM
(SELECT
 TRIM(LEFT(tt.province,2)) AS province,
 COUNT(DISTINCT tt.sys_customer_id) AS user_counts
FROM
 data_check.customer_data_city tt
GROUP BY
 province
ORDER BY
 province,
 user_counts DESC) tt
GROUP BY
 tt.province)tt
LEFT JOIN data_check.province_population_shop pp
ON LEFT(tt.province,2)=LEFT(pp.province,2)
WHERE
pp.province IS NOT NULL
ORDER BY
user_per_shop;

-- 地域分布: 客单价分布
USE data_check;
DROP TABLE
IF EXISTS user_province_price;
CREATE TABLE user_province_price AS
SELECT
 pp.province as std_province,
 tt.province as tt_province,
 tt.province_counts AS province_counts,
 tt.province_amount AS province_amount,
 ROUND(tt.province_amount/tt.province_counts,0) AS user_price
FROM
(SELECT
 tt.province,
 SUM(tt.user_amoiunt) AS province_amount,
 SUM(tt.user_counts) AS province_counts
FROM
(SELECT
 TRIM(LEFT(tt.province,2)) AS province,
 ROUND(SUM(tt.pay_amount),1) AS user_amoiunt,
 COUNT(DISTINCT tt.sys_customer_id) AS user_counts
FROM
 data_check.customer_data_city tt
GROUP BY
 province
ORDER BY
 province,
 user_counts DESC) tt
GROUP BY
 tt.province)tt
LEFT JOIN data_check.province_population_shop pp
ON LEFT(tt.province,2)=LEFT(pp.province,2)
WHERE
pp.province IS NOT NULL
ORDER BY
user_price;

-- 全景扫描-三茶市场-======================================
-- santea:全景扫描-三茶市场-基础数据
USE data_check;
SET @sys_customer_index = ROUND((SELECT MAX(sys_customer_id) FROM crm_kd.kd_customer),0);
DROP TABLE 
IF EXISTS santea_data_city;
CREATE TABLE santea_data_city AS
SELECT
 tt.sys_customer_id,
 tt.out_nick,
 tt.customer_name,
 tt.mobile,
 tt.province,
 tt.city,
 tt.plat_from_type,
 tt.develop_time,
 tt.first_pay_time,
 tt.last_pay_time,
 tt.pay_times,
 tt.pay_amount
FROM(
(SELECT
 tt.sys_customer_id,
 tt.out_nick,
 tt.customer_name,
 tt.mobile,
 tt.province,
 tt.city,
 tt.plat_from_type,
 tt.develop_time,
 tt.first_pay_time,
 tt.last_pay_time,
 tt.pay_times,
 tt.pay_amount
FROM
 crm_kd.kd_customer tt
WHERE
 tt.sys_customer_id IN(SELECT
 tt.sys_customer_id
FROM
(SELECT
 tt.sys_customer_id
FROM
 crm_kd.kd_order tt 
WHERE
 tt.title REGEXP '纤纤|减肥|菁|润|常'
  )tt
GROUP BY
 tt.sys_customer_id
ORDER BY
 tt.sys_customer_id) -- 曾经购买三茶用户信息筛选
AND
 DATE_FORMAT(tt.develop_time,"%Y-%m-%d") BETWEEN "2016-01-01" AND "2017-10-01")
UNION
(SELECT
 @sys_customer_index:= @sys_customer_index+1 AS sys_customer_id,
 tt.out_nick,
 tt.customer_name,
 tt.mobile,
 tt.province,
 tt.city,
 tt.plat_from_type,
 MIN(tt.develop_time) AS develop_time,
 MIN(tt.order_pay_time) AS first_pay_time,
 MAX(tt.order_pay_time) AS last_pay_time,
 COUNT(DISTINCT order_id) AS pay_times,
 SUM(tt.order_pay_amount) AS pay_amount
FROM
(SELECT
 tt.`用户名` AS out_nick,
 tt.`订单ID辅助列` AS order_id,
 tt.`收货人` AS customer_name,
 tt.`收货人电话` AS mobile,
 tt.`收货省份` AS province,
 tt.`收货城市` AS city,
 tt.商品名称,
 DATE_FORMAT(tt.`下单时间`,"%Y-%m-%d %h:%m:%s") AS develop_time,
 DATE_FORMAT(tt.`付款时间`,"%Y-%m-%d %h:%m:%s") AS order_pay_time,
 ROUND(tt.GMV,1) AS order_pay_amount,
 "46" AS plat_from_type
FROM
 master_data.mas_fristdrug_data2 tt
WHERE 
 tt.`用户名`<> 'N'
AND
 tt.商品名称 REGEXP '纤纤茶|减肥茶|常菁茶|常润茶' )tt
GROUP BY
 tt.out_nick,
 tt.customer_name,
 tt.mobile,
 tt.province,
 tt.city,
 tt.plat_from_type
ORDER BY
 pay_times DESC))tt;

-- 地域分布: 消费者渗透力
USE data_check;
DROP TABLE
IF EXISTS santea_user_province;
CREATE TABLE santea_user_province AS
SELECT
 pp.province as std_province,
 tt.province as tt_province,
 tt.province_counts AS santea_counts,
 zz.province_counts AS province_counts,
 ROUND(100*tt.province_counts/zz.province_counts,1) AS user_per_wan
FROM
(SELECT
 tt.province,
 SUM(tt.user_counts) AS province_counts
FROM
(SELECT
 TRIM(LEFT(tt.province,2)) AS province,
 COUNT(DISTINCT tt.sys_customer_id) AS user_counts
FROM
 data_check.santea_data_city tt
GROUP BY
 province
ORDER BY
 province,
 user_counts DESC) tt
GROUP BY
 tt.province)tt
LEFT JOIN data_check.province_population pp
ON LEFT(tt.province,2)=LEFT(pp.province,2)
LEFT JOIN data_check.user_province_test zz
ON LEFT(tt.province,2)=LEFT(zz.std_province,2)
WHERE
pp.province IS NOT NULL
ORDER BY
user_per_wan;

-- 地域分布: 区域分布


USE data_check;

DROP TABLE
IF EXISTS santea_province_shop;

CREATE TABLE santea_province_shop AS SELECT
	pp.province AS std_province,
	tt.province AS tt_province,
	tt.province_counts AS province_counts,
	pp.shops AS shops,
	ROUND(
		10 * tt.province_counts / pp.shops,
		0
	) AS user_per_shop
FROM
	(
		SELECT
			tt.province,
			SUM(tt.user_counts) AS province_counts
		FROM
			(
				SELECT
					TRIM(LEFT(tt.province, 2)) AS province,
					COUNT(DISTINCT tt.sys_customer_id) AS user_counts
				FROM
					data_check.santea_data_city tt
				GROUP BY
					province
				ORDER BY
					province,
					user_counts DESC
			) tt
		GROUP BY
			tt.province
	) tt
LEFT JOIN data_check.province_population_shop pp ON LEFT (tt.province, 2) = LEFT (pp.province, 2)
WHERE
	pp.province IS NOT NULL
ORDER BY
	user_per_shop;

-- 地域分布: 客单价分布
USE data_check;
DROP TABLE
IF EXISTS santea_province_price;
CREATE TABLE santea_province_price AS
SELECT
 pp.province as std_province,
 tt.province as tt_province,
 tt.province_counts AS province_counts,
 tt.province_amount AS province_amount,
 ROUND(tt.province_amount/tt.province_counts,0) AS user_price
FROM
(SELECT
 tt.province,
 SUM(tt.user_amoiunt) AS province_amount,
 SUM(tt.user_counts) AS province_counts
FROM
(SELECT
 TRIM(LEFT(tt.province,2)) AS province,
 ROUND(SUM(tt.pay_amount),1) AS user_amoiunt,
 COUNT(DISTINCT tt.sys_customer_id) AS user_counts
FROM
 data_check.santea_data_city tt
GROUP BY
 province
ORDER BY
 province,
 user_counts DESC) tt
GROUP BY
 tt.province)tt
LEFT JOIN data_check.province_population_shop pp
ON LEFT(tt.province,2)=LEFT(pp.province,2)
WHERE
pp.province IS NOT NULL
ORDER BY
user_price;


-- 全景扫描-新产品市场-======================================
-- santea:全景扫描-新产品市场-基础数据
USE data_check;
SET @sys_customer_index = ROUND((SELECT MAX(sys_customer_id) FROM crm_kd.kd_customer),0);
DROP TABLE 
IF EXISTS newproduct_data_city;
CREATE TABLE newproduct_data_city AS
SELECT
 tt.sys_customer_id,
 tt.out_nick,
 tt.customer_name,
 tt.mobile,
 tt.province,
 tt.city,
 tt.plat_from_type,
 tt.develop_time,
 tt.first_pay_time,
 tt.last_pay_time,
 tt.pay_times,
 tt.pay_amount
FROM(
(SELECT
 tt.sys_customer_id,
 tt.out_nick,
 tt.customer_name,
 tt.mobile,
 tt.province,
 tt.city,
 tt.plat_from_type,
 tt.develop_time,
 tt.first_pay_time,
 tt.last_pay_time,
 tt.pay_times,
 tt.pay_amount
FROM
 crm_kd.kd_customer tt
WHERE
 tt.sys_customer_id IN(SELECT
 tt.sys_customer_id
FROM
(SELECT
 tt.sys_customer_id
FROM
 crm_kd.kd_order tt 
WHERE
 tt.title REGEXP '代餐|益生菌|维生素|左旋肉碱咖啡|酵素'
  )tt
GROUP BY
 tt.sys_customer_id
ORDER BY
 tt.sys_customer_id) -- 曾经购买新产品用户信息筛选
AND
 DATE_FORMAT(tt.develop_time,"%Y-%m-%d") BETWEEN "2016-01-01" AND "2017-10-01")
UNION
(SELECT
 @sys_customer_index:= @sys_customer_index+1 AS sys_customer_id,
 tt.out_nick,
 tt.customer_name,
 tt.mobile,
 tt.province,
 tt.city,
 tt.plat_from_type,
 MIN(tt.develop_time) AS develop_time,
 MIN(tt.order_pay_time) AS first_pay_time,
 MAX(tt.order_pay_time) AS last_pay_time,
 COUNT(DISTINCT order_id) AS pay_times,
 SUM(tt.order_pay_amount) AS pay_amount
FROM
(SELECT
 tt.`用户名` AS out_nick,
 tt.`订单ID辅助列` AS order_id,
 tt.`收货人` AS customer_name,
 tt.`收货人电话` AS mobile,
 tt.`收货省份` AS province,
 tt.`收货城市` AS city,
 tt.商品名称,
 DATE_FORMAT(tt.`下单时间`,"%Y-%m-%d %h:%m:%s") AS develop_time,
 DATE_FORMAT(tt.`付款时间`,"%Y-%m-%d %h:%m:%s") AS order_pay_time,
 ROUND(tt.GMV,1) AS order_pay_amount,
 "46" AS plat_from_type
FROM
 master_data.mas_fristdrug_data2 tt
WHERE 
 tt.`用户名`<> 'N'
AND
 tt.商品名称 REGEXP '代餐|益生菌|维生素|左旋肉碱咖啡|酵素' )tt
GROUP BY
 tt.out_nick,
 tt.customer_name,
 tt.mobile,
 tt.province,
 tt.city,
 tt.plat_from_type
ORDER BY
 pay_times DESC))tt;

-- 地域分布: 消费者渗透力
USE data_check;
DROP TABLE
IF EXISTS newproduct_user_province;
CREATE TABLE newproduct_user_province AS
SELECT
 pp.province as std_province,
 tt.province as tt_province,
 tt.province_counts AS province_counts,
 pp.population AS population,
 ROUND(10*tt.province_counts/pp.population,0) AS user_per_wan
FROM
(SELECT
 tt.province,
 SUM(tt.user_counts) AS province_counts
FROM
(SELECT
 TRIM(LEFT(tt.province,2)) AS province,
 COUNT(DISTINCT tt.sys_customer_id) AS user_counts
FROM
 data_check.newproduct_data_city tt
GROUP BY
 province
ORDER BY
 province,
 user_counts DESC) tt
GROUP BY
 tt.province)tt
LEFT JOIN data_check.province_population pp
ON LEFT(tt.province,2)=LEFT(pp.province,2)
WHERE
pp.province IS NOT NULL
ORDER BY
user_per_wan;

-- 地域分布: 区域分布
USE data_check;
DROP TABLE
IF EXISTS newproduct_province_shop;
CREATE TABLE newproduct_province_shop AS
SELECT
 pp.province as std_province,
 tt.province as tt_province,
 tt.province_counts AS province_counts,
 pp.shops AS shops,
 ROUND(10*tt.province_counts/pp.shops,0) AS user_per_shop
FROM
(SELECT
 tt.province,
 SUM(tt.user_counts) AS province_counts
FROM
(SELECT
 TRIM(LEFT(tt.province,2)) AS province,
 COUNT(DISTINCT tt.sys_customer_id) AS user_counts
FROM
 data_check.newproduct_data_city tt
GROUP BY
 province
ORDER BY
 province,
 user_counts DESC) tt
GROUP BY
 tt.province)tt
LEFT JOIN data_check.province_population_shop pp
ON LEFT(tt.province,2)=LEFT(pp.province,2)
WHERE
pp.province IS NOT NULL
ORDER BY
user_per_shop;

-- 地域分布: 客单价分布
USE data_check;
DROP TABLE
IF EXISTS newproduct_province_price;
CREATE TABLE newproduct_province_price AS
SELECT
 pp.province as std_province,
 tt.province as tt_province,
 tt.province_counts AS province_counts,
 tt.province_amount AS province_amount,
 ROUND(tt.province_amount/tt.province_counts,0) AS user_price
FROM
(SELECT
 tt.province,
 SUM(tt.user_amoiunt) AS province_amount,
 SUM(tt.user_counts) AS province_counts
FROM
(SELECT
 TRIM(LEFT(tt.province,2)) AS province,
 ROUND(SUM(tt.pay_amount),1) AS user_amount,
 COUNT(DISTINCT tt.sys_customer_id) AS user_counts
FROM
 data_check.newproduct_data_city tt
GROUP BY
 province
ORDER BY
 province,
 user_counts DESC) tt
GROUP BY
 tt.province)tt
LEFT JOIN data_check.province_population_shop pp
ON LEFT(tt.province,2)=LEFT(pp.province,2)
WHERE
pp.province IS NOT NULL
ORDER BY
user_price;




SELECT
 SUM(tt.pay_amount)/COUNT(tt.sys_customer_id)
FROM
 data_check.santea_data_city tt;
-- 消费者行为研究$产品偏好度-文本分析


-- 单一省份================================================================
-- 浙江
-- 浙江分布: 消费者渗透力
USE data_check;
DROP TABLE
IF EXISTS user_zhejiang_test;
CREATE TABLE user_zhejiang_test AS
SELECT
 pp.province as std_province,
 pp.city as std_city,
 tt.province_counts AS province_counts,
 pp.population AS population,
 ROUND(10*tt.province_counts/pp.population,0) AS user_per_wan
FROM
(SELECT
 tt.province,
 tt.city,
 SUM(tt.user_counts) AS province_counts
FROM
(SELECT
 TRIM(LEFT(tt.province,2)) AS province,
 TRIM(LEFT(tt.city,2)) AS city,
 COUNT(DISTINCT tt.sys_customer_id) AS user_counts
FROM
 data_check.customer_data_city tt
GROUP BY
 province,
 city
ORDER BY
 province,
 user_counts DESC) tt
GROUP BY
 tt.province,
 tt.city
HAVING
 tt.province ='浙江')tt
LEFT JOIN data_check.zhejiang pp
ON CONCAT(LEFT(tt.province,2),left(tt.city,2))=CONCAT(LEFT(pp.province,2),left(pp.city,2))
WHERE
pp.province IS NOT NULL
ORDER BY
user_per_wan;


-- 地域分布: 区域分布
USE data_check;
DROP TABLE
IF EXISTS user_zhejiang_shop;
CREATE TABLE user_zhejiang_shop AS
SELECT
 pp.province as std_province,
 pp.city as std_city,
 tt.province_counts AS province_counts,
 pp.shops AS shops,
 ROUND(10*tt.province_counts/pp.shops,0) AS user_per_shop
FROM
(SELECT
 tt.province,
 tt.city,
 SUM(tt.user_counts) AS province_counts
FROM
(SELECT
 TRIM(LEFT(tt.province,2)) AS province,
 TRIM(LEFT(tt.city,2)) AS city,
 COUNT(DISTINCT tt.sys_customer_id) AS user_counts
FROM
 data_check.customer_data_city tt
GROUP BY
 province,
 city
ORDER BY
 province,
 user_counts DESC) tt
GROUP BY
 tt.province,
 tt.city
HAVING
 tt.province ='浙江')tt
LEFT JOIN data_check.zhejiang pp
ON CONCAT(LEFT(tt.province,2),left(tt.city,2))=CONCAT(LEFT(pp.province,2),left(pp.city,2))
WHERE
pp.province IS NOT NULL
ORDER BY
user_per_shop;


-- 浙江分布: 客单价
USE data_check;
DROP TABLE
IF EXISTS user_zhejiang_price;
CREATE TABLE user_zhejiang_price AS
SELECT
 pp.province as std_province,
 pp.city as std_city,
 tt.province_counts AS province_counts,
 tt.province_amount AS province_amount,
 ROUND(tt.province_amount/tt.province_counts,0) AS user_price
FROM
(SELECT
 tt.province,
 tt.city,
 SUM(tt.user_amount) AS province_amount,
 SUM(tt.user_counts) AS province_counts
FROM
(SELECT
 TRIM(LEFT(tt.province,2)) AS province,
 TRIM(LEFT(tt.city,2)) AS city,
 ROUND(SUM(tt.pay_amount),1) AS user_amount,
 COUNT(DISTINCT tt.sys_customer_id) AS user_counts
FROM
 data_check.customer_data_city tt
GROUP BY
 province,
 city
ORDER BY
 province,
 user_counts DESC) tt
GROUP BY
 tt.province,
 tt.city
HAVING
 tt.province ='浙江')tt
LEFT JOIN data_check.zhejiang pp
ON CONCAT(LEFT(tt.province,2),left(tt.city,2))=CONCAT(LEFT(pp.province,2),left(pp.city,2))
WHERE
pp.province IS NOT NULL
ORDER BY
user_price;



-- 三茶用户-浙江区域-消费者渗透
USE data_check;
DROP TABLE
IF EXISTS santea_user_zhejiang;
CREATE TABLE santea_user_zhejiang AS
SELECT
 pp.province as std_province,
 pp.city as std_city,
 tt.province_counts AS santea_counts,
 zz.province_counts AS province_counts,
 ROUND(100*tt.province_counts/zz.province_counts,1) AS user_per_wan
FROM
(SELECT
 tt.province,
 tt.city,
 SUM(tt.user_counts) AS province_counts
FROM
(SELECT
 TRIM(LEFT(tt.province,2)) AS province,
 TRIM(LEFT(tt.city,2)) AS city,
 COUNT(DISTINCT tt.sys_customer_id) AS user_counts
FROM
 data_check.santea_data_city tt
GROUP BY
 province,
 city
ORDER BY
 province,
 user_counts DESC) tt
GROUP BY
 tt.province,
 tt.city
HAVING
 tt.province ='浙江')tt
LEFT JOIN data_check.zhejiang pp
ON CONCAT(LEFT(tt.province,2),left(tt.city,2))=CONCAT(LEFT(pp.province,2),left(pp.city,2))
LEFT JOIN data_check.user_zhejiang_test zz
ON CONCAT(LEFT(tt.province,2),left(tt.city,2))=CONCAT(LEFT(zz.std_province,2),left(zz.std_city,2))
WHERE
pp.province IS NOT NULL
ORDER BY
user_per_wan;

-- 三茶用户-浙江分布: 客单价
USE data_check;
DROP TABLE
IF EXISTS santea_zhejiang_price;
CREATE TABLE santea_zhejiang_price AS
SELECT
 pp.province as std_province,
 pp.city as std_city,
 tt.province_counts AS province_counts,
 tt.province_amount AS province_amount,
 ROUND(tt.province_amount/tt.province_counts,0) AS user_price
FROM
(SELECT
 tt.province,
 tt.city,
 SUM(tt.user_amount) AS province_amount,
 SUM(tt.user_counts) AS province_counts
FROM
(SELECT
 TRIM(LEFT(tt.province,2)) AS province,
 TRIM(LEFT(tt.city,2)) AS city,
 ROUND(SUM(tt.pay_amount),1) AS user_amount,
 COUNT(DISTINCT tt.sys_customer_id) AS user_counts
FROM
 data_check.santea_data_city tt
GROUP BY
 province,
 city
ORDER BY
 province,
 user_counts DESC) tt
GROUP BY
 tt.province,
 tt.city
HAVING
 tt.province ='浙江')tt
LEFT JOIN data_check.zhejiang pp
ON CONCAT(LEFT(tt.province,2),left(tt.city,2))=CONCAT(LEFT(pp.province,2),left(pp.city,2))
WHERE
pp.province IS NOT NULL
ORDER BY
user_price;


-- 消费者行为研究$购物篮分析-关联规则=======================================================================

SELECT
 t1.sys_customer_id,
 t1.address,
 t1.develop_time,
 t2.shop_name
FROM
 crm_kd.kd_customer t1,crm_kd.kd_customer_ext  t2
WHERE
 t1.sys_customer_id = t2.sys_customer_id
AND t1.sys_customer_id IN
(SELECT
 tt.sys_customer_id
FROM
 crm_kd.kd_customer tt
WHERE
 tt.plat_from_type = 40 
AND MONTH(tt.develop_time)>7);
 
