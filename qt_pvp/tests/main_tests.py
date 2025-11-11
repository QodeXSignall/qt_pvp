import unittest, asyncio
from main_operator import Main
from qt_pvp.cms_interface import cms_api

class TestCase(unittest.TestCase):

    def test_get_pics_before_after(self):
        async def runner():
            d = Main()
            return await d.get_channels_to_download_pics("/Tracker/Видео выгрузок/К180КЕ702/2025.10.08/К180КЕ702_2025.10.08 06.23.33-06.24")
        res = asyncio.run(runner())
        print("\nRES", res)


    @unittest.SkipTest
    def test_get_img(self):
        async def runner():
            d = Main()
            return await cms_api.download_video(
                d.jsession,
                reg_id="108411",
                channel_id=2,
                year=2025,
                month=9,
                day=1,
                start_sec=59400,
                end_sec=59400,

            )
        res = asyncio.run(runner())
        print("\nRES", res)

if __name__ == '__main__':
    unittest.main()


# Лишний конец
[2,8,0.5,0,0,0]