import unittest, asyncio
from main_operator import Main
from qt_pvp.cms_interface import cms_api

class TestCase(unittest.TestCase):
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
