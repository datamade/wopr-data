if __name__ == "__main__":
    # Reformats the datetime value from a given column
    # args: column_index, from_format, to_format
    import sys
    import csv
    from datetime import datetime
    idx = sys.argv[1]
    from_fmt = sys.argv[2]
    to_fmt = sys.argv[3]
    reader = csv.reader(sys.stdin)
    rows = []
    for row in reader:
        val = row[int(idx)]
        row[int(idx)] = datetime.strptime(val.strip(), from_fmt).strftime(to_fmt)
        rows.append(row)
    writer = csv.writer(sys.stdout)
    writer.writerows(rows)
