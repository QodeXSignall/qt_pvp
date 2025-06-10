from main_operator import main_funcs
from main_operator import Main
import unittest


class TestCase(unittest.TestCase):
    def get_interests(self, reg_id, start_time, end_time):
        d = Main()
        reg_info = main_funcs.get_reg_info(
            reg_id)
        interests = d.get_interests(reg_id, reg_info, start_time, end_time)
        return interests

    def test_get_interests(self):
        reg_id = "018270348452"
        #reg_id = "K630AX702"
        interests = self.get_interests(
            reg_id,
            #"2025-05-18 08:20:00", "2025-05-18 09:50:00"
            "2025-05-16 10:40:00", "2025-05-16 10:59:00"
            )
        print("Interests")
        for interest in interests:
            print(interest)

if __name__ == '__main__':
    unittest.main()
