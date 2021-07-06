from scripts.common import DirectoryFields
import pandas as pd
import aiohttp
import asyncio
import pickle

class ScrapeFile:

    def __init__(self, df):
        pass

    async def fetch(self, session, index, row):
        success = False
        err_msg = ""
        for _ in range(5):
            try:
                async with session.get(row["URL"]) as response:
                    text = await response.text()
                    await self.extract_tags(text,index)
                    success = True
                    break
            except Exception as e:
                err_msg = e
        if not success:
            print(f"get error: {err_msg}")
            self.df.loc[index,"License Number"] = "FAILURE"

    async def main(self, top_index, bot_index):
        tasks = list()
        start_index = top_index
        headers = ({'User-Agent':
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
            (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',\
            'Accept-Language': 'en-US, en;q=0.5'})
        async with aiohttp.ClientSession(headers=headers) as session:
            while top_index <= bot_index and top_index - start_index < self.segment_size:
                task = asyncio.ensure_future(self.fetch(session, top_index, self.df.loc[top_index]))
                tasks.append(task)
                top_index+=1
                # print(top_index)
            await asyncio.gather(*tasks)
        
    def get_data(self, overwrite = True):
        self.links = pickle.load(open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/dos/temp/links-{self.industry}-0", "rb"))
        self.links = self.links[12:143]
        self.df["URL"] = self.links
        self.df["Industry"] = self.industry
        if overwrite == False:
            self.df = pickle.load(open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/dos/temp/df-{self.industry}-0", "rb"))
            self.org_df = self.df[~(self.df["License Number"].eq("FAILURE"))]
            self.df = self.df[self.df["License Number"].eq("FAILURE")]
        print(len(self.df))
        self.df=self.df.reset_index(drop=True)
        
        top_index = self.df.iloc[0].name
        bot_index = self.df.iloc[-1].name
        while top_index <= bot_index:
            asyncio.run(self.main(top_index,bot_index))
            top_index+=self.segment_size
            print(top_index)

        if overwrite == False:
            self.df = pd.concat([self.org_df,self.df], ignore_index=True)

        pickle.dump(self.df, open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/dos/temp/df-{self.industry}-0", "wb"))
        cleaned_file_path = f"{DirectoryFields.LOCAL_LOCUS_PATH}data/dos/{self.industry}/{self.department}_{self.industry}_{date.today()}_scrape.csv"
        self.df.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_ALL)

    
    def save_pages(self):
        pickle.dump(self.links, open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/dos/temp/links-{self.industry}-0", "wb"))
        pickle.dump(self.start_clicks, open(f"{DirectoryFields.LOCAL_LOCUS_PATH}data/dos/temp/var-{self.industry}-0", "wb"))
        print(f"clicks {self.start_clicks}")
        print(f"links {len(self.links)}")
