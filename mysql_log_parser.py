import os

db_users = [
    "root",
    # "db_user_name_2",
    # "db_user_name_3"
]

mysql_log_file_path = "mysql-slow.log"
extract_to_dir_path = "outputs"

just_that_takes_more_time_than = 1 # int second
without_repetition = 1

if os.path.exists(extract_to_dir_path):
    for filename in os.listdir(extract_to_dir_path):
        file_path = os.path.join(extract_to_dir_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)

        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))
else:
    os.makedirs(extract_to_dir_path)

with open(mysql_log_file_path, "r") as input_file:
    for user_name in db_users:
        print(user_name)
        allow = 0
        row = 0
        prev = ""
        db_name = ""
        query = ""
        seen = set()
        user_line = ""

        with open(os.path.join(extract_to_dir_path, f"{user_name}_logs.txt"), 'w') as output_file:
            for line in input_file:
                if "use _sar_" in line:
                    db_name = line
                    continue

                if "# User@Host: " in line:
                    query = ""
                    user_line = ""
                    if user_name in line:
                        row += 1
                        user_line += prev
                        user_line += line
                        allow = 1
                    else:
                        allow = 0
                    continue

                if "# Time: " in line:
                    prev = line
                    if allow == 1:
                        if without_repetition == 1:
                            if not query in seen:
                                seen.add(query)
                                output_file.write("\n\n")
                                output_file.write(
                                    "##############################################################################  " + str(
                                        row))
                                output_file.write("\n")
                                output_file.write(db_name)
                                output_file.write(user_line)
                                output_file.write(query)
                        else:
                            output_file.write("\n\n")
                            output_file.write(
                                "##############################################################################  " + str(
                                    row))
                            output_file.write("\n")
                            output_file.write(db_name)
                            output_file.write(user_line)
                            output_file.write(query)

                    query = ""
                    user_line = ""
                    allow = 0
                    continue

                if "# Query_time: " in line:
                    if allow == 1:
                        if just_that_takes_more_time_than > 0:
                            if int(line[14:15]) < just_that_takes_more_time_than:
                                query = ""
                                user_line = ""
                                allow = 0
                                continue

                        user_line += line
                    continue

                if "SET timestamp=" in line:
                    if allow == 1:
                        user_line += line
                    continue

                if allow == 1:
                    query += line.strip() + " "

            input_file.seek(0)
            if allow == 1:
                if without_repetition == 1:
                    if not query in seen:
                        seen.add(query)
                        output_file.write("\n\n")
                        output_file.write(
                            "##############################################################################  " + str(
                                row))
                        output_file.write("\n")
                        output_file.write(db_name)
                        output_file.write(user_line)
                        output_file.write(query)
                else:
                    output_file.write(
                        "##############################################################################  " + str(row))
                    output_file.write("\n")
                    output_file.write(db_name)
                    output_file.write(user_line)
                    output_file.write(query)

print("=====")
print("Done")
