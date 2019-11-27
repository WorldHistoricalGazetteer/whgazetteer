from selenium import webdriver
import unittest

class NewVisitorTest(unittest.TestCase):  

    def setUp(self):  
        self.browser = webdriver.Firefox()

    def tearDown(self):  
        self.browser.quit()

    def homepage_loads(self):  
        # homepage loads
        self.browser.get('http://localhost:8000')

        # title keyword
        self.assertIn('WHG', self.browser.title)  

        # more basic stuff
       

if __name__ == '__main__':  
    unittest.main(warnings='ignore')  