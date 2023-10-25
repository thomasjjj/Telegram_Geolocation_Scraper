from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
import pandas as pd
import datetime
import re

#### telegram credentials
api_id = #enter telegram api id
api_hash = #enter telegram api hash
client = TelegramClient('anon', api_id, api_hash)

#### scrapes posts from channels
def channel_scraper(channel_links, date_limit, output_path):
    
    #### check format on date_limit
    date_limit = datetime.datetime.strptime(date_limit, "%Y-%m-%d")

    #### lists for output DF
    d1,d2,d3,d4,d5,d6,d7 = [],[],[],[],[],[],[]


    #### grid extract regex
    coordinate_pattern = re.compile(r'(-?\d+\.\d+),\s*(-?\d+\.\d+)')    

    if type(channel_links) is list:
        for cl in channel_links:
            async def main():

                channel = await client.get_entity(cl)

                async for message in client.iter_messages(channel, reverse= True, offset_date= date_limit):

                    #### CHECK IF MESSAGE CONTAINS GRIDS
                    message_text = str(message.message)

                    coordinates_matches = coordinate_pattern.findall(message_text)

                    for grid in coordinates_matches:
                        #### APPEND LAT/LON to DF
                        latitude, longitude = grid

                        d6.append(latitude)
                        d7.append(longitude)

                        #### APPEND MESSAGE ID and MESSAGE CONTENT
                        message_id = message.id

                        d1.append(message_id)
                        d2.append(message_text)

                        if isinstance(message.media, (MessageMediaPhoto, MessageMediaDocument)):
                            if isinstance(message.media, MessageMediaPhoto):
                                d3.append('photo')
                            elif isinstance(message.media, MessageMediaDocument):
                                d3.append('video/mp4')
                        else:
                            d3.append('unknown')


                        #### FORMAT PUBLISH DATE AND APPEND TO LIST
                        date = str(message.date).split(' ')[0]
                        date_format = datetime.datetime.strptime(date, '%Y-%m-%d').strftime("%Y-%m-%d")
                        d4.append(date_format)

                        #### FORMAT AND APPEND SOURCE
                        source = f't.me/{cl}/{message_id}'
                        d5.append(source)

            with client:
                client.loop.run_until_complete(main())
    else:
        async def main():

            channel = await client.get_entity(channel_links)

            async for message in client.iter_messages(channel, reverse= True, offset_date= date_limit):

                #### CHECK IF MESSAGE CONTAINS GRIDS
                message_text = str(message.message)

                coordinates_matches = coordinate_pattern.findall(message_text)

                for grid in coordinates_matches:
                    #### APPEND LAT/LON to DF
                    latitude, longitude = grid

                    d6.append(latitude)
                    d7.append(longitude)

                    #### APPEND MESSAGE ID and MESSAGE CONTENT
                    message_id = message.id

                    d1.append(message_id)
                    d2.append(message_text)
                        
                    if isinstance(message.media, (MessageMediaPhoto, MessageMediaDocument)):
                        if isinstance(message.media, MessageMediaPhoto):
                            d3.append('photo')
                        elif isinstance(message.media, MessageMediaDocument):
                            d3.append('video/mp4')
                    else:
                        d3.append('unknown')

                    #### FORMAT PUBLISH DATE AND APPEND TO LIST
                    date = str(message.date).split(' ')[0]
                    date_format = datetime.datetime.strptime(date, '%Y-%m-%d').strftime("%Y-%m-%d")
                    d4.append(date_format)

                    #### FORMAT AND APPEND SOURCE
                    source = f't.me/{channel_links}/{message_id}'
                    d5.append(source)

        with client:
            client.loop.run_until_complete(main())

    output = pd.DataFrame({'message_id':d1, 'message_content':d2, 'message_media_type':d3, 'message_published_at':d4, 'message_source':d5, 'latitude':d6, 'longitude':d7})

    output.to_csv(output_path, index= False)