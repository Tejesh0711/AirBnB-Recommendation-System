listings = LOAD 'hdfs://localhost:8020/input/Boston/listings_cleaned.txt' USING PigStorage('\t');
listingsandprice = FOREACH listings GENERATE $0 as listing_id,$60 as price;
priceGrouped = GROUP listingsandprice by price;
byPriceCount = FOREACH priceGrouped GENERATE FLATTEN(group) as (no_listings), COUNT($1);
STORE byPriceCount INTO 'hdfs://localhost:8020/op/listingsbyprice' USING PigStorage('\t');