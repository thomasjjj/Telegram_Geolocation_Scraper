# Telegram Coordinates Extractor

### Example
Using Exports from the Telegram Channels "WarArchive_ua" and "military_u_geo", this map was produced. This tool is agnostic to context, source accuracy, and bias. All results should be provisional and only used as a starting point.
![image](https://github.com/thomasjjj/Telegram_Geolocation_Scraper/assets/118008765/ce32041a-fb98-4173-acaa-9b49f05f962f)

## Table of Contents
- [Overview](#overview)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
  - [Exporting Chat from Telegram](#exporting-chat-from-telegram)
  - [Running the Script](#running-the-script)
- [Viewing Coordinates on Google Earth](#viewing-coordinates-on-google-earth)

## Overview
This Python script automates the extraction of geographic coordinates from a Telegram channel's exported chat history. The extracted data is saved as a CSV file, including additional details like the post's ID, date, message content, and media type.

## Requirements
- Python 3.x
- Tkinter (usually comes pre-installed with Python)
- pandas library

## Installation
1. Ensure that you have Python installed. If not, download and install it from [python.org](https://www.python.org/).
2. Install pandas by running `pip install pandas` in your command line or terminal.

## Usage

### Exporting Chat from Telegram
1. Open the Telegram desktop application.
2. Navigate to the channel whose data you wish to export.
3. Click on the three vertical dots (⋮) at the top-right corner to reveal a dropdown menu.
4. Choose `Export chat history`.
5. In the dialogue box that appears:
    - Under 'Format', select `JSON`.
    - Disable all media downloads by unchecking the boxes next to each media type.
6. Click `Export` to save the JSON file on your computer.

### Running the Script
1. Save the Python script in a location of your choice.
2. Open your command line or terminal and navigate to the directory where the script is saved.
3. Run the script by typing `python script_name.py` (replace `script_name` with the actual name of the script).
4. Follow the on-screen prompts to:
    - Name the output CSV file.
    - Select the JSON file to process.
    - Specify the directory to save the CSV file.
    - Enter the base URL for post links.

## Viewing Coordinates on Google Earth
1. Open Google Earth on your computer.
2. Navigate to `File` > `Import`.
3. In the dialogue box, select the CSV file that you generated with this script.
4. Google Earth will automatically read the latitude and longitude columns, plotting the coordinates on the map.

By following these steps, you can visualise the geographic locations mentioned in the Telegram posts directly on Google Earth.
