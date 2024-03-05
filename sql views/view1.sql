SELECT c.sector,
GROUP_CONCAT(DISTINCT rl.community),
COUNT(DISTINCT rl.id) AS number_of_listings,
AVG(rl.price) AS avg_listing_price,
AVG(c.crime_pct),
AVG (sr.school_rating) AS avg_school_rating,
GROUP_CONCAT(DISTINCT s.name || ' (' || sr.school_rating || ')') AS schools_with_ratings
FROM rental_listings rl
INNER JOIN schools_within_catchment swc ON swc.listing_id = rl.id
INNER JOIN listing_with_crime lwc ON lwc.listing_id = rl.id
INNER JOIN crime c ON c.id = lwc.crime_id
INNER JOIN schools s ON s.school_id = swc.school_id
INNER JOIN school_ranking sr ON sr.school_id = s.school_id
WHERE --rl.price <= 2100 
	--AND rl.beds >= 1 
	rl.is_active = True 
	AND sr.school_rating >= 7 
	AND sr.school_group = 'elementary'
	AND c.crime_pct < 0.5
GROUP BY c.sector
ORDER BY avg_school_rating DESC
