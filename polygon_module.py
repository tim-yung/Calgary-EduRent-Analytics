from shapely.geometry import Point, Polygon
import matplotlib.pyplot as plt
import math

def check_catchment(polygon_points, user_lat, user_long):
    polygon = Polygon(polygon_points)
    point = Point(user_long, user_lat)
    return point.within(polygon)

def plot_result(polygon_points, user_lat, user_long, ax):
    polygon = Polygon(polygon_points)
    x,y = polygon.exterior.xy
    ax.plot(x, y)
    ax.plot(user_long, user_lat, 'ro') # 'ro' makes the point red and round
    is_within = check_catchment(polygon_points, user_lat, user_long)
    ax.annotate(f"Is within: {is_within}", (user_long, user_lat), textcoords="offset points", xytext=(-10,10), ha='center')

def check_user_in_polygons(polygon_dict, user_lat, user_long, headless=True):
    point_in_polygon = False
    for polygon_number, polygon_points in polygon_dict.items():
        
        result = check_catchment(polygon_points, user_lat, user_long)
        if result:
            point_in_polygon = True
            if not headless:
                fig, ax = plt.subplots(figsize=(15, 15))
                plot_result(polygon_points, user_lat, user_long, ax)  
                plt.show()
    if not point_in_polygon and not headless:
        fig, ax = plt.subplots(figsize=(15, 15))
        for polygon_number, polygon_points in polygon_dict.items():
            plot_result(polygon_points, user_lat, user_long, ax)
        plt.tight_layout()
        plt.show()
    return point_in_polygon