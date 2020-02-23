from argparse import ArgumentParser
from collections import namedtuple
from csv import writer
from itertools import accumulate, chain, groupby, tee
from math import atan2, cos, sin, degrees
from pathlib import Path

from gpxpy import parse
from gpxpy.geo import Location, length, elevation_angle, simplify_polyline
from shapely.geometry import LineString


PointNT = namedtuple('PointNT',
                     ['time', 'latitude', 'longitude', 'elevation', 'heart_rate',
                      'session_id', 'distance', 'duration', 'bearing', 'elevation_angle', 'elevation_diff',
                      'instantaneous_speed', 'cumulative_downhill', 'cumulative_uphill', 'cumulative_distance',
                      'cumulative_duration', 'rolling_average_speed'],
                     defaults=[None]*7 + [0]*4 + [None])


class Point(PointNT, Location):
    def __init__(self, *args, **kwargs):
        super()


PointHull = namedtuple('PointHull', ['session_id', 'vertex_id', 'latitude', 'longitude'])


def parse_heart_rate(gpx_point, gpx_nsmap):
    if gpx_point.extensions:
        return int(gpx_point.extensions[0].find('gpxtpx:hr', gpx_nsmap).text)


def ingest(gpx_filename):
    with gpx_filename.open() as gpx_file:
        gpx = parse(gpx_file, '1.1')
        for track in gpx.tracks:
            for segment in track.segments:
                for point in segment.points:
                    yield Point(time=point.time,
                                latitude=point.latitude,
                                longitude=point.longitude,
                                elevation=point.elevation,
                                heart_rate=parse_heart_rate(point, gpx.nsmap)
                                )


def clean(points):
    timeline = sorted(points, key=lambda p: p.time)
    point_by_time = groupby(timeline, key=lambda p: p.time)
    timeline_strictly_chronological = (next(point_group) for _, point_group in point_by_time)
    point_by_location = groupby(timeline_strictly_chronological, key=lambda p: (p.latitude, p.longitude, p.elevation))
    point_distinct = list(next(point_group) for _, point_group in point_by_location)
    return simplify_polyline(point_distinct, 5)


def bearing_from_north_clockwise(location1, location2):
    # https://www.igismap.com/formula-to-find-bearing-or-heading-angle-between-two-points-latitude-longitude/
    longitude_diff = location2.longitude - location1.longitude
    x = cos(location2.latitude) * sin(longitude_diff)
    y = cos(location1.latitude) * sin(location2.latitude) -\
        sin(location1.latitude) * cos(location2.latitude) * cos(longitude_diff)
    return degrees(atan2(x, y))


def enrich(points):
    (timeline, timeline_next) = tee(points, 2)
    point = next(timeline_next)
    first_time_in_session = point.time
    yield point._replace(session_id=first_time_in_session)
    for point, point_next in zip(timeline, timeline_next):
        distance = length(locations=[point, point_next], _3d=True)
        duration = (point_next.time - point.time).total_seconds()
        yield point_next._replace(
            session_id=first_time_in_session,
            distance=distance,
            duration=duration,
            bearing=bearing_from_north_clockwise(point, point_next),
            instantaneous_speed=distance / duration,
            elevation_diff=point_next.elevation - point.elevation,
            elevation_angle=elevation_angle(point, point_next, radians=False)
        )


def cumulate(point1, point2):
    cumulative_distance = point1.cumulative_distance + point2.distance
    cumulative_duration = point1.cumulative_duration + point2.duration
    return point2._replace(
        cumulative_downhill=point1.cumulative_downhill + point2.elevation_diff if point2.elevation_diff < 0 else point1.cumulative_downhill,
        cumulative_uphill=point1.cumulative_uphill + point2.elevation_diff if point2.elevation_diff > 0 else point1.cumulative_uphill,
        cumulative_distance=cumulative_distance,
        cumulative_duration=point1.cumulative_duration + point2.duration,
        rolling_average_speed=cumulative_distance / cumulative_duration
    )


def transform(points):
    point_timeline = sorted(points, key=lambda p: p.time)
    points_enriched = enrich(point_timeline)
    points_cumulated = accumulate(points_enriched, func=cumulate)
    yield from points_cumulated


def process_session(gpx_filename):
    points = ingest(gpx_filename)
    points_clean = clean(points)
    points_transformed = transform(points_clean)
    return points_transformed


def extract_hull(points):
    points_by_session = groupby(points, lambda p: p.session_id)
    for session_id, points in points_by_session:
        line = LineString([(p.latitude, p.longitude) for p in points])
        polygon = line.buffer(.005).buffer(-.0045).simplify(.0005)
        vertices = polygon.boundary.coords
        for vertex_id, vertex in enumerate(vertices):
            latitude, longitude = vertex
            yield PointHull(session_id=session_id,
                            vertex_id=vertex_id,
                            latitude=latitude,
                            longitude=longitude
                            )


def write(sessions, path_points, model=Point):
    with path_points.open('w', newline='') as points_file:
        csv_writer = writer(points_file, delimiter='^')
        csv_writer.writerow(model._fields)
        for session in sessions:
            for point in session:
                csv_writer.writerow(point)


def main_session_points(input_gpx_directory, output):
    path_to_gpx_files = Path(input_gpx_directory)
    gpx_filenames = chain(path_to_gpx_files.rglob('*.gpx.xml'), path_to_gpx_files.rglob('*.gpx'))
    sessions_transformed = map(process_session, gpx_filenames)
    path_to_output_sessions = Path(output)
    write(sessions_transformed, path_to_output_sessions)


def main_session_hulls(input_gpx_directory, output):
    path_to_gpx_files = Path(input_gpx_directory)
    gpx_filenames = chain(path_to_gpx_files.rglob('*.gpx.xml'), path_to_gpx_files.rglob('*.gpx'))
    sessions_transformed = map(process_session, gpx_filenames)
    sessions_hulls = map(extract_hull, sessions_transformed)
    path_to_output_hulls = Path(output)
    write(sessions_hulls, path_to_output_hulls, model=PointHull)


def parse_arguments():
    parser = ArgumentParser(description='Ingest and transform GPX files where 1 session = 1 file.')
    parser.add_argument('input', metavar='IN', type=str,
                        help='path to the directory containing the GPX files.')
    parser.add_argument('output', metavar='OUT', type=str,
                        help='path to the output file containing the prepared data.')
    parser.add_argument('--hull', dest='main', action='store_const',
                        const=main_session_hulls, default=main_session_points,
                        help='compute a hull for each session (default: ingest and enrich sessions).')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_arguments()
    args.main(args.input, args.output)
