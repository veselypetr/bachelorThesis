import csv
import os
from datetime import datetime, timedelta
import pandas as pd
import reverse_geocoder as rg


def merge_csv():
    fout = open("merged.csv", "w", encoding="utf-8")
    # first file:
    for line in open("export_heatmap_2019_200211_000000000000.csv"):
        fout.write(line)
    # now the rest:
    for num in range(1, 11):
        print(num)
        if num == 10:
            f = open("export_heatmap_2019_200211_0000000000" + str(num) + ".csv", encoding="utf-8")
        else:
            f = open("export_heatmap_2019_200211_00000000000" + str(num) + ".csv", encoding="utf-8")
        f.__next__()  # skip the header
        for line in f:
            fout.write(line)
    fout.close()


def undupe():
    found = {}
    with open("merged.csv", "r", encoding="utf-8") as input_file, open("unduped.csv", "w", newline='',
                                                                       encoding="utf-8") as output_file:
        reader = csv.reader(input_file, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
        writer = csv.writer(output_file)
        line_num = 0
        for row in reader:
            timestamp = row[0]
            user_id = row[5]
            gps = row[12]
            if timestamp not in found:
                found[timestamp] = {}
            if user_id not in found[timestamp]:
                found[timestamp][user_id] = {}
            if gps not in found[timestamp][user_id]:
                found[timestamp][user_id][gps] = True
                if line_num >= 1:
                    gps_list = row[12].split(',')
                    row = row[:12]
                    row.append(unhose_latitude(gps_list[0]))
                    row.append(unhose_longitude(gps_list[1]))
                writer.writerow(row)
            line_num += 1
            if line_num % 100000 == 0:
                print(line_num)
            # if line_num >= 75000:
            #     break


def aggregate_days_old_data():
    new_rows = {}
    with open("oldData.csv", "r") as input_file, open("aggregate_days_old_data.csv", "w", newline='',
                                                                        encoding="utf-8") as output_file:
        reader = csv.reader(input_file, quotechar='"', delimiter=';', quoting=csv.QUOTE_ALL, skipinitialspace=True)
        writer = csv.writer(output_file)
        line_num = 0
        next(reader, None)
        for row in reader:
            date_string = row[0]
            user_id = row[1]
            dev_language = row[4]
            dev_system = row[5]
            gps = row[3]
            gps_list = gps.split(',')
            gps_lat = (unhose_latitude(gps_list[0]))
            gps_lon = (unhose_longitude(gps_list[1]))
            date_list = date_string.split('T')
            date_string = date_list[0]

            tourist = True
            if 'cs' in dev_language or 'cz' in dev_language:
                tourist = False

            if date_string not in new_rows:
                new_rows[date_string] = {}
            if gps_lat not in new_rows[date_string]:
                new_rows[date_string][gps_lat] = {}
            if gps_lon not in new_rows[date_string][gps_lat]:
                new_rows[date_string][gps_lat][gps_lon] = {}
            if dev_language not in new_rows[date_string][gps_lat][gps_lon]:
                new_rows[date_string][gps_lat][gps_lon][dev_language] = {}
            if dev_system not in new_rows[date_string][gps_lat][gps_lon][dev_language]:
                new_rows[date_string][gps_lat][gps_lon][dev_language][dev_system] = {}
            if tourist not in new_rows[date_string][gps_lat][gps_lon][dev_language][dev_system]:
                new_rows[date_string][gps_lat][gps_lon][dev_language][dev_system][tourist] = 1
            else:
                new_rows[date_string][gps_lat][gps_lon][dev_language][dev_system][tourist] += 1
            line_num += 1

            if line_num == 0:
                writer.writerow(['date', 'gps_lat', 'gps_lon', 'dev_language', 'dev_system', 'tourist', 'count'])
            line_num += 1
            if line_num % 100000 == 0:
                print(line_num)
            # if line_num >= 75000:
            #     break

        writer.writerow(['date', 'gps_lat', 'gps_lon', 'dev_language', 'dev_system', 'tourist', 'count'])
        for date_string in new_rows:
            for gps_lat in new_rows[date_string]:
                for gps_lon in new_rows[date_string][gps_lat]:
                    for dev_language in new_rows[date_string][gps_lat][gps_lon]:
                        for dev_system in new_rows[date_string][gps_lat][gps_lon][dev_language]:
                            for tourist in new_rows[date_string][gps_lat][gps_lon][dev_language][dev_system]:
                                num = new_rows[date_string][gps_lat][gps_lon][dev_language][dev_system][tourist]
                                writer.writerow(
                                    [date_string, gps_lat, gps_lon, dev_language, dev_system, tourist, num])

def aggregate_times_old_data():
    new_rows = {}
    with open("oldData.csv", "r") as input_file, open("times_old_data.csv", "w", newline='',
                                                                        encoding="utf-8") as output_file:
        reader = csv.reader(input_file, quotechar='"', delimiter=';', quoting=csv.QUOTE_ALL, skipinitialspace=True)
        writer = csv.writer(output_file)
        line_num = 0
        next(reader, None)
        for row in reader:
            time = row[6]
            date_string = row[0]
            user_id = row[1]
            dev_language = row[4]
            dev_system = row[5]
            gps = row[3]
            gps_list = gps.split(',')
            gps_lat = (unhose_latitude(gps_list[0]))
            gps_lon = (unhose_longitude(gps_list[1]))

            time_object = datetime.strptime(date_string.replace('T', ' ').replace('Z', ''), '%Y-%m-%d %H:%M:%S.%f')
            if time_object.minute < 15:
                time_object = time_object.replace(minute=0)
            elif time_object.minute < 45:
                time_object = time_object.replace(minute=30)
            else:
                time_object = time_object.replace(minute=0, hour=(time_object.hour + 1) % 24)
            time_string = time_object.strftime('%H:%M')

            tourist = True
            if 'cs' in dev_language or 'cz' in dev_language:
                tourist = False

            if time_string not in new_rows:
                new_rows[time_string] = {}
            if gps_lat not in new_rows[time_string]:
                new_rows[time_string][gps_lat] = {}
            if gps_lon not in new_rows[time_string][gps_lat]:
                new_rows[time_string][gps_lat][gps_lon] = {}
            if dev_language not in new_rows[time_string][gps_lat][gps_lon]:
                new_rows[time_string][gps_lat][gps_lon][dev_language] = {}
            if dev_system not in new_rows[time_string][gps_lat][gps_lon][dev_language]:
                new_rows[time_string][gps_lat][gps_lon][dev_language][dev_system] = {}
            if tourist not in new_rows[time_string][gps_lat][gps_lon][dev_language][dev_system]:
                new_rows[time_string][gps_lat][gps_lon][dev_language][dev_system][tourist] = 1
            else:
                new_rows[time_string][gps_lat][gps_lon][dev_language][dev_system][tourist] += 1
            line_num += 1

            if line_num == 0:
                writer.writerow(['date', 'gps_lat', 'gps_lon', 'dev_language', 'dev_system', 'tourist', 'count'])
            line_num += 1
            if line_num % 100000 == 0:
                print(line_num)
            # if line_num >= 75000:
            #     break

        writer.writerow(['date', 'gps_lat', 'gps_lon', 'dev_language', 'dev_system', 'tourist', 'count'])
        for date_string in new_rows:
            for gps_lat in new_rows[date_string]:
                for gps_lon in new_rows[date_string][gps_lat]:
                    for dev_language in new_rows[date_string][gps_lat][gps_lon]:
                        for dev_system in new_rows[date_string][gps_lat][gps_lon][dev_language]:
                            for tourist in new_rows[date_string][gps_lat][gps_lon][dev_language][dev_system]:
                                num = new_rows[date_string][gps_lat][gps_lon][dev_language][dev_system][tourist]
                                writer.writerow(
                                    [date_string, gps_lat, gps_lon, dev_language, dev_system, tourist, num])


def unhose_latitude(lat_str):
    lat_str = lat_str.strip()
    lat_str = lat_str.replace("Â", "")
    lat_str = lat_str.replace("°", "")
    if "N" in lat_str:
        return lat_str.replace("N", "")
    if "S" in lat_str:
        lat_str = lat_str.replace("S", "")
        return "-" + lat_str


def unhose_longitude(long_str):
    long_str = long_str.strip()
    long_str = long_str.replace("Â", "")
    long_str = long_str.replace("°", "")
    if "E" in long_str:
        return long_str.replace("E", "")
    if "W" in long_str:
        long_str = long_str.replace("W", "")
        return "-" + long_str


def aggregate_days():
    new_rows = {}
    with open("unduped.csv", "r", encoding="utf-8") as input_file, open("heatmap2.csv", "w", newline='',
                                                                        encoding="utf-8") as output_file:
        reader = csv.reader(input_file, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True, )
        writer = csv.writer(output_file)
        line_num = 0
        next(reader, None)
        for row in reader:
            date_string = datetime.utcfromtimestamp(int(row[0]) / 1000000).date().strftime('%Y-%m-%d')
            gps_lat = float(row[12])
            gps_lon = float(row[13])
            dev_language = row[10]
            dev_system = row[8]

            tourist = True

            if 'cs' in row[10] or 'cz' in row[10]:
                tourist = False

            if date_string not in new_rows:
                new_rows[date_string] = {}
            if gps_lat not in new_rows[date_string]:
                new_rows[date_string][gps_lat] = {}
            if gps_lon not in new_rows[date_string][gps_lat]:
                new_rows[date_string][gps_lat][gps_lon] = {}
            if dev_language not in new_rows[date_string][gps_lat][gps_lon]:
                new_rows[date_string][gps_lat][gps_lon][dev_language] = {}
            if dev_system not in new_rows[date_string][gps_lat][gps_lon][dev_language]:
                new_rows[date_string][gps_lat][gps_lon][dev_language][dev_system] = {}
            if tourist not in new_rows[date_string][gps_lat][gps_lon][dev_language][dev_system]:
                new_rows[date_string][gps_lat][gps_lon][dev_language][dev_system][tourist] = 1
            else:
                new_rows[date_string][gps_lat][gps_lon][dev_language][dev_system][tourist] += 1
            line_num += 1
            if line_num % 100000 == 0:
                print(line_num)
            # if line_num >= 75000:
            #     break

        writer.writerow(['date', 'gps_lat', 'gps_lon', 'dev_language', 'dev_system', 'tourist', 'count'])
        for date_string in new_rows:
            for gps_lat in new_rows[date_string]:
                for gps_lon in new_rows[date_string][gps_lat]:
                    for dev_language in new_rows[date_string][gps_lat][gps_lon]:
                        for dev_system in new_rows[date_string][gps_lat][gps_lon][dev_language]:
                            for tourist in new_rows[date_string][gps_lat][gps_lon][dev_language][dev_system]:
                                num = new_rows[date_string][gps_lat][gps_lon][dev_language][dev_system][tourist]
                                writer.writerow(
                                    [date_string, gps_lat, gps_lon, dev_language, dev_system, tourist, num])


def get_unique():
    new_rows = {}
    with open("heatmap.csv", "r", encoding="utf-8") as input_file, open("days.csv", "w", newline='',
                                                                        encoding="utf-8") as output_file:
        reader = csv.reader(input_file, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True, )
        writer = csv.writer(output_file)
        line_num = 0
        next(reader, None)
        days = {}
        for row in reader:
            if row[0] not in days:
                days[row[0]] = 0
            days[row[0]] += int(row[3]) / 100
        for day in sorted(days.keys()):
            print(day + ': ' + str(int(days[day])))


def create_polylines():
    with open("polylines.csv", "w", newline='', encoding="utf-8") as output_file:

        writer = csv.writer(output_file)
        writer.writerow(['user_id', 'datetime', 'date', 'dev_system', 'dev_language', 'tourist', 'points'])
        # data = pd.read_csv('unduped_head.csv')
        data = pd.read_csv('unduped.csv')
        print('starting sort')
        data = data.sort_values(by=['user_id', 'event_timestamp'])
        print('sort done')

        polyline = []
        row_data = []

        old_row = None
        for line_num, row in data.iterrows():
            if old_row is not None:
                time_object_old = datetime.utcfromtimestamp(int(old_row['event_timestamp']) / 1000000)
            time_object_new = datetime.utcfromtimestamp(int(row['event_timestamp']) / 1000000)

            if (old_row is not None and old_row['user_id'] == row['user_id']
                    and (time_object_new - time_object_old) < timedelta(minutes=60)
                    and abs(old_row['gps_lat'] - row['gps_lat']) < 0.5
                    and abs(old_row['gps_lon'] - row['gps_lon']) < 0.5
            ):
                if old_row['gps_lat'] != row['gps_lat'] and old_row['gps_lon'] != row['gps_lon']:
                    polyline.append((row['gps_lat'], row['gps_lon']))
            else:
                tourist = True
                if 'cs' in row['device_language'] or 'cz' in row['device_language']:
                    tourist = False
                if len(polyline) > 1:
                    writer.writerow(row_data + [polyline])
                row_data = [row['user_id'], time_object_new.strftime('%Y-%m-%d %H:%M'),
                            time_object_new.strftime('%Y-%m-%d'), row['operating_system'], row['device_language'],
                            tourist]
                polyline = [(row['gps_lat'], row['gps_lon'])]

            old_row = row
            if line_num % 100000 == 0:
                print(line_num)

        if len(polyline) > 1:
            writer.writerow(row_data + [polyline])


def aggregate_times():
    new_rows = {}
    with open("unduped.csv", "r", encoding="utf-8") as input_file, open("times.csv", "w", newline='',
                                                                        encoding="utf-8") as output_file:
        reader = csv.reader(input_file, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True, )
        writer = csv.writer(output_file)
        line_num = 0
        next(reader, None)
        for row in reader:
            time_object = datetime.utcfromtimestamp(int(row[0]) / 1000000)
            if time_object.minute < 15:
                time_object = time_object.replace(minute=0)
            elif time_object.minute < 45:
                time_object = time_object.replace(minute=30)
            else:
                time_object = time_object.replace(minute=0, hour=(time_object.hour + 1) % 24)
            time_string = time_object.strftime('%H:%M')

            gps_lat = float(row[12])
            gps_lon = float(row[13])
            dev_language = row[10]
            dev_system = row[8]
            tourist = True
            if 'cs' in row[10] or 'cz' in row[10]:
                tourist = False

            if time_string not in new_rows:
                new_rows[time_string] = {}
            if gps_lat not in new_rows[time_string]:
                new_rows[time_string][gps_lat] = {}
            if gps_lon not in new_rows[time_string][gps_lat]:
                new_rows[time_string][gps_lat][gps_lon] = {}
            if dev_language not in new_rows[time_string][gps_lat][gps_lon]:
                new_rows[time_string][gps_lat][gps_lon][dev_language] = {}
            if dev_system not in new_rows[time_string][gps_lat][gps_lon][dev_language]:
                new_rows[time_string][gps_lat][gps_lon][dev_language][dev_system] = {}
            if tourist not in new_rows[time_string][gps_lat][gps_lon][dev_language][dev_system]:
                new_rows[time_string][gps_lat][gps_lon][dev_language][dev_system][tourist] = 1
            else:
                new_rows[time_string][gps_lat][gps_lon][dev_language][dev_system][tourist] += 1
            line_num += 1
            if line_num % 100000 == 0:
                print(line_num)
            # if line_num >= 75000:
            #     break

        writer.writerow(['date', 'gps_lat', 'gps_lon', 'dev_language', 'dev_system', 'tourist', 'count'])
        for time_string in new_rows:
            for gps_lat in new_rows[time_string]:
                for gps_lon in new_rows[time_string][gps_lat]:
                    for dev_language in new_rows[time_string][gps_lat][gps_lon]:
                        for dev_system in new_rows[time_string][gps_lat][gps_lon][dev_language]:
                            for tourist in new_rows[time_string][gps_lat][gps_lon][dev_language][dev_system]:
                                num = new_rows[time_string][gps_lat][gps_lon][dev_language][dev_system][tourist]
                                writer.writerow(
                                    [time_string, gps_lat, gps_lon, dev_language, dev_system, tourist, num])


def get_countries():
    with open("unduped.csv", "r", encoding="utf-8") as input_file, open("countries.csv", "w", newline='',
                                                                        encoding="utf-8") as output_file:
        reader = csv.reader(input_file, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
        writer = csv.writer(output_file)
        line_num = 0
        next(reader)
        header = ['timestamp', 'user_id', 'os', 'device_language', 'gps_lat', 'gps_lon', 'name', 'cc', 'admin1',
                  'admin2']
        writer.writerow(header)
        for row in reader:
            gps_lat = row[12]
            gps_lon = row[13]

            rg_out = rg.search((gps_lat, gps_lon), mode=1)
            if rg_out:
                writer.writerow([row[0], row[5], row[8], row[10], row[12], row[13], rg_out[0].get('name', ''),
                                 rg_out[0].get('cc', ''), rg_out[0].get('admin1', ''), rg_out[0].get('admin2', '')])
            line_num += 1
            if line_num % 100000 == 0:
                print(line_num)
            # if line_num >= 75000:
            #     break


def aggregate_countries():
    new_rows = {}
    with open("countries.csv", "r", encoding="utf-8") as input_file, open("countries_agg.csv", "w", newline='',
                                                                          encoding="utf-8") as output_file:
        reader = csv.reader(input_file, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True, )
        writer = csv.writer(output_file)
        line_num = 0
        next(reader, None)
        for row in reader:
            date_string = datetime.utcfromtimestamp(int(row[0]) / 1000000).date().strftime('%Y-%m-%d')
            dev_language = row[3]
            dev_system = row[2]
            city = row[6]
            country = row[7]

            if date_string not in new_rows:
                new_rows[date_string] = {}
            if country not in new_rows[date_string]:
                new_rows[date_string][country] = {}
            if city not in new_rows[date_string][country]:
                new_rows[date_string][country][city] = {}
            if dev_language not in new_rows[date_string][country][city]:
                new_rows[date_string][country][city][dev_language] = {}
            if dev_system not in new_rows[date_string][country][city][dev_language]:
                new_rows[date_string][country][city][dev_language][dev_system] = 1
            else:
                new_rows[date_string][country][city][dev_language][dev_system] += 1
            line_num += 1
            if line_num % 100000 == 0:
                print(line_num)
            # if line_num >= 75000:
            #     break

        writer.writerow(['date', 'country', 'city', 'dev_language', 'dev_system', 'count'])
        for date_string in new_rows:
            for country in new_rows[date_string]:
                for city in new_rows[date_string][country]:
                    for dev_language in new_rows[date_string][country][city]:
                        for dev_system in new_rows[date_string][country][city][dev_language]:
                            num = new_rows[date_string][country][city][dev_language][dev_system]
                            writer.writerow([date_string, country, city, dev_language, dev_system, num])


def country_hist():
    new_rows = {}
    with open("countries.csv", "r", encoding="utf-8") as input_file, open("countries_agg_hist.csv", "w", newline='',
                                                                          encoding="utf-8") as output_file:
        reader = csv.reader(input_file, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL,
                            skipinitialspace=True, )
        writer = csv.writer(output_file)
        line_num = 0
        next(reader, None)
        for row in reader:
            date_string = datetime.utcfromtimestamp(int(row[0]) / 1000000).date().strftime('%Y-%m-%d')
            country = row[7]

            if date_string not in new_rows:
                new_rows[date_string] = {}
            if country not in new_rows[date_string]:
                new_rows[date_string][country] = 1
            else:
                new_rows[date_string][country] += 1
            line_num += 1
            if line_num % 100000 == 0:
                print(line_num)
            # if line_num >= 75000:
            #     break

        writer.writerow(['date', 'country', 'count'])
        for date_string in new_rows:
            for country in new_rows[date_string]:
                num = new_rows[date_string][country]
                writer.writerow([date_string, country, num])


# apply the maximum absolute scaling in Pandas using the .abs() and .max() methods
def maximum_absolute_scaling(df):
    # copy the dataframe
    df_scaled = df.copy()
    # apply maximum absolute scaling
    df_scaled['count'] = df_scaled['count'] / df_scaled['count'].abs().max()
    #df_scaled['count'] = (df_scaled.groupby['count'] / df_scaled['count'].abs().max()) * -1
    return df_scaled

def create_differential_heatmap(df1, df2):
    #pd.merge(df1, df2, on=['date', 'gps_lon', 'gps_lat']).set_index(['id', 'name']).sum(axis=1)
    df_new = pd.concat([df1, df2]).groupby(['date', 'gps_lat', 'gps_lon', 'dev_language', 'dev_system', 'tourist']).sum().reset_index()

    return df_new

aggregate_days()