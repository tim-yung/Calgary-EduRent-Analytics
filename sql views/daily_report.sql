SELECT
    rl.community,
    rl."type",
    rl.beds,
    rl.sq_feet, 
    rl.price,
    rl.cats,
    rl.dogs,
    rl.baths,
    c.crime_count,
    ROUND(c.crime_pct, 2) AS crime_percentile,
    (SELECT s.name FROM schools s
        JOIN school_ranking sr ON sr.school_id = s.school_id
        WHERE sr.school_id = swwz.school_id
        ORDER BY sr.school_rating DESC
        LIMIT 1) AS highest_rated_school_name,
    MAX(sr.school_rating) AS highest_school_rating,    
    CASE 
        WHEN sl.school_id IS NOT NULL THEN CONCAT('Required in ', sl.school_year)
        ELSE 'Not Required'
    END AS lottery_requirement,
    CONCAT('https://www.rentfaster.ca', rl.link) AS link
FROM
    rental_listings rl
    INNER JOIN schools_within_walk_zone swwz ON swwz.listing_id = rl.id
    INNER JOIN listing_with_crime lwc ON lwc.listing_id = rl.id
    INNER JOIN crime c ON c.id = lwc.crime_id
    INNER JOIN schools s ON s.school_id = swwz.school_id
    INNER JOIN school_ranking sr ON sr.school_id = s.school_id
    LEFT JOIN school_lottery sl ON sl.school_id = s.school_id 
WHERE
    sr.school_group = 'elementary'
    AND rl.price <= 2100
    AND rl.is_active = True
    AND c.crime_pct < 0.5
GROUP BY
    rl.id
HAVING
    highest_school_rating >= 8
ORDER BY
    highest_school_rating DESC,
    price ASC