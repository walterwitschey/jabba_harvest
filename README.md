# The Jabba Harvester

JabbaLogParse: DICOM Log Parser and Analyzer (jabba_log_parse.py)

The JabbaLogParse class in this Python script is designed to parse and analyze DICOM log files generated by a DICOM server. The log files contain information about various DICOM operations such as C-STORE requests and transfers. The purpose of this code is to extract relevant information from the log files, perform analysis on the data, and generate summarized reports containing insights about DICOM transfers.
Usage Instructions

    Dependencies: Make sure you have the following dependencies installed in your Python environment:
        pandas

    You can install them using the following command:

pip install pandas

Run the Script: Execute the script using a Python interpreter. The script takes several command-line arguments:

    -s or --server: Paths to server log files (space-separated).
    -o or --output: Path to the output file (optional).
    -k or --key: Path to the accession-to-UID key CSV file (optional).
    -t or --tree: Directory containing DICOM tree files (optional).
    -d or --dicom: Base directory of DICOM files (optional).
    -p or --processed: Base directory of processed files (required).

Example command:

c

    python script_name.py -s server_log_1.log server_log_2.log -t dicom_tree_dir -d dicom_files_dir -p processed_files_dir

Code Explanation

The script defines a JabbaLogParse class that provides methods for parsing and analyzing DICOM server log files. It includes functions to parse different types of log lines, process log files, analyze log data, and obtain DICOM tree data.

Here's a brief overview of some key methods in the class:

    ParseAILogFile: This method parses a single AI log file, extracting relevant information from each line and organizing it into rows of a DataFrame. It also handles cases where information spans multiple lines.

    ParseAILogs: This method parses a list of processed AI log files, extracting relevant data and populating a DataFrame.

    ParseServerLogLine: This method parses a single line from a server log file, specifically looking for C-STORE requests. It extracts information such as timestamp, module, level, and relevant UIDs.

    ParseServerLog: This method processes server log files, extracting C-STORE request data and storing it in a DataFrame. It also supports adding DICOM tree data to the log data.

    AnalyzeParsedServerLog: This method performs analysis on the parsed server log data. It calculates transfer times, counts of instances, and incorporates DICOM tree data if available.

    GetTreeData: This method extracts DICOM tree data from JSON files and returns data in DataFrame format.

    main: The main function handles command-line arguments and orchestrates the parsing and analysis process.

Example Output

After running the script, it will process the provided server log files, perform analysis, and generate summarized data. The specific output format and details will depend on the analysis performed by the script and the presence of DICOM tree data.
Note

    The script requires log files and additional data (like DICOM trees) as inputs. Ensure that you provide the correct paths for these data sources.
    The script's functionality heavily depends on the specifics of the log files and data structures used. Make sure you have a clear understanding of how your data is organized before using this script.

AI Log Parser and Analyzer (ailogparser.py)

This Python script is designed to parse and analyze log files generated by an AI system. The log files contain information about various events that occur during the execution of the AI system. The purpose of this code is to extract relevant information from the log files, perform analysis on the data, and generate a summarized CSV report containing useful insights.
Usage Instructions

    Dependencies: Make sure you have the following dependencies installed in your Python environment:
        pandas
        numpy

    You can install them using the following command:

    pip install pandas numpy

Log Files: Place your AI log files in a directory. The script expects the log files to have filenames that match the pattern *_ai_log_*.

Configure Log Directory: In the script, modify the logdir variable to point to the directory containing your AI log files.

Run the Script: Execute the script using a Python interpreter. Navigate to the directory where the script is located and run:

    python script_name.py

    The script will process the log files, perform analysis, and generate a CSV file named parsed_ai_logs.csv.

Code Explanation

    Parsing Log Lines (ParseAILogLine)
        This function takes a single log line as input and extracts relevant information using space as a delimiter.
        It converts the timestamp and other data into a dictionary format if the log line corresponds to a relevant event (like pipeline start, pipeline end, push start, push stop).

    Parsing AI Logs (ParseAILogs)
        This function takes a list of filenames as input.
        It iterates through each log file, reads lines, and calls ParseAILogLine to extract data.
        The extracted data is accumulated in a list of dictionaries.

    Analyzing AI Logs (AnalyzeAILogs)
        This function takes the list of dictionaries obtained from ParseAILogs as input.
        It converts the list of dictionaries into a pandas DataFrame.
        The DataFrame is pivoted to organize events as columns and accession/series numbers as indices.
        Time differences (in minutes) between different events are calculated and added as new columns in the DataFrame.

    Main Execution
        The script first retrieves the list of AI log files in the specified directory using regular expressions.
        It then processes the log files using the functions defined above.
        The analyzed data is stored in a DataFrame, and the DataFrame is saved as a CSV file.

Example Output

After running the script, you will get a CSV file named parsed_ai_logs.csv that contains summarized data from the AI log files. The CSV file will have columns like accession, series_no, pipeline_start, pipeline_end, push_start, push_stop, pipeline_time, push_time, and AI_plus_push.
Note

    This script assumes a specific log file naming convention (*_ai_log_*). Make sure your log files follow this convention for accurate processing.
    The script processes logs sequentially, so it's suitable for moderate-sized log files. For very large log files, additional optimizations might be needed.

For any further customization or modifications, refer to the comments in the code and the pandas documentation for additional information.



*********
Usage instructions provided by chatGPT with the following prompt:
Explain the purpose of the following code in detail, along with usage instructions, required dependencies and other pertinent information for a novice user of the software.  Please format for a readme file in github.
