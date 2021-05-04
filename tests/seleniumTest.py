import pytest
import selenium
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
import sys
import unittest
import time
import os
import threading

url = 'http://localhost:8080'
fakeJobPath = os.path.join('tests', 'fakeJob.py')

allThreads = []


class PeakLearnerTests(unittest.TestCase):

    def setUp(self) -> None:
        try:
            self.driver = webdriver.Chrome()
        except WebDriverException:
            self.driver = webdriver.Chrome('/buildtools/webdriver/chromdriver')
        self.driver.set_window_size(1280, 667)

    def test_homepage(self):
        self.driver.get(url)


    def test_upload_hub(self):
        self.driver.get("http://localhost:8080/")
        self.driver.find_element(By.ID, "navbarDropdown").click()
        self.driver.find_element(By.LINK_TEXT, "Upload New Hub").click()
        self.driver.find_element(By.ID, "hubUrl").click()
        self.driver.find_element(By.ID, "hubUrl").send_keys(
            "https://rcdata.nau.edu/genomic-ml/PeakSegFPOP/labels/H3K4me3_TDH_ENCODE/hub.txt")
        self.driver.find_element(By.CSS_SELECTOR, "input:nth-child(6)").click()
        wait = WebDriverWait(self.driver, 15)

        wait.until(EC.title_contains('JBrowse'))


    def test_zlabels_add(self):
        self.driver.get("http://localhost:8080/1/H3K4me3_TDH_ENCODE/")
        wait = WebDriverWait(self.driver, 15)
        wait.until(EC.presence_of_element_located((By.ID, "hierarchicalTrackPane")))

        element = self.driver.find_element(By.ID, "hierarchicalTrackPane")

        checkboxes = element.find_elements(By.TAG_NAME, 'input')

        tracks = []

        # Load all available tracks
        for checkbox in checkboxes:
            checkbox.screenshot('ss/test.png')
            parent = checkbox.find_element(By.XPATH, '..')
            trackName = parent.text

            # Not sure why I need to check for this but okay
            if trackName == '' or 'Input' in trackName:
                continue
            checkbox.click()
            tracks.append(trackName)
            trackId = 'track_%s' % trackName
            wait.until(EC.presence_of_element_located((By.ID, trackId)))


        # Move to defined location

        searchbox = self.driver.find_element(By.ID, 'search-box')

        chromDropdown = searchbox.find_element(By.ID, 'search-refseq')

        chromDropdown.click()

        menu = self.driver.find_element(By.ID, 'dijit_form_Select_0_menu')

        options = menu.find_elements(By.XPATH, './/*')

        # Go to chr3 chrom
        for option in options:
            if option.tag_name == 'tr':
                label = option.get_attribute('aria-label')

                if label is None:
                    continue

                # Space at end or something
                if label.strip() == 'chr3':
                    option.click()
                    break


        assert 'chr3' in self.driver.title

        # Move location now
        elem = searchbox.find_element(By.ID, 'widget_location')
        nav = elem.find_element(By.ID, 'location')
        nav.clear()

        # not sure why navigating here goes to the url below but it does it seemingly every time so
        nav.send_keys('93504855..194041961')

        go = searchbox.find_element(By.ID, 'search-go-btn_label')

        go.click()

        assert "93462708..192840581" in self.driver.title

        # scroll to top of page
        element = self.driver.find_element(By.CLASS_NAME, 'vertical_position_marker')

        action = ActionChains(self.driver)

        action.move_to_element(element)

        # Not sure why I need an extra 100 pixels but it works
        y = element.location.get('y') + 100

        action.drag_and_drop_by_offset(element, 0, -y)

        action.perform()

        # Zoom in closer
        for i in range(4):
            self.zoomIn()

        time.sleep(3)

        self.addPeak(2255, width=80)

        # 2050 start
        self.dragTrack(2050, 2900)

        self.zoomIn()

        self.addPeak(2440, width=80)

        self.zoomOut()

        self.addLabel('noPeak', 2100, 2400)

        self.zoomOut()

        self.dragTrack(2050, 2800)

        self.zoomIn()

        self.addPeak(2595)

        self.addLabel('noPeak', 2100, 2570)

        self.dragTrack(2050, 2800)

        self.addPeak(2752)

        self.addPeak(2189)

        self.addLabel('noPeak', 2215, 2550)

        self.zoomOut()

        self.dragTrack(2050, 2350)

        self.zoomIn()

        self.addPeak(2868)

        self.addPeak(2227)

        self.addPeak(2140)

        self.addLabel('noPeak', 2300, 2800)

        self.dragTrack(2050, 2800)

        self.addPeak(2235)

        self.addLabel('noPeak', 2265, 2850)

        self.zoomOut()

        self.dragTrack(2050, 2350)

        self.zoomIn()

        self.addPeak(2241)

        self.addLabel('noPeak', 2285, 2525)

        self.addPeak(2622)

        assert 1 == 0

    def addPeak(self, midPoint, width=40):
        labelWidth = width / 2

        self.addLabel('peakStart', midPoint - labelWidth, midPoint - 1)

        self.addLabel('peakEnd', midPoint, midPoint + labelWidth)

    def addLabel(self, labelType, start, end):
        wait = WebDriverWait(self.driver, 15)
        wait.until(EC.presence_of_element_located((By.ID, "track_aorta_ENCFF115HTK")))

        labelDropdown = self.driver.find_element(By.ID, 'current-label')

        labelDropdown.click()

        labelMenu = self.driver.find_element(By.ID, 'current-label_dropdown')

        options = labelMenu.find_elements(By.XPATH, './/*')

        for option in options:
            if option.tag_name == 'tr':
                label = option.get_attribute('aria-label')

                if label is None:
                    continue

                # Space at end or something
                if label.strip() == labelType:
                    option.click()
                    break

        element = self.driver.find_element(By.ID, 'highlight-btn')

        element.click()

        track = self.driver.find_element(By.ID, 'track_aorta_ENCFF115HTK')

        action = ActionChains(self.driver)

        action.move_to_element_with_offset(track, start, 50)

        action.click_and_hold().perform()

        action.move_by_offset(end - start, 50)

        action.release().perform()

        wait.until(EC.presence_of_element_located((By.ID, "track_aorta_ENCFF115HTK")))

    def dragTrack(self, start, end):
        wait = WebDriverWait(self.driver, 15)

        wait.until(EC.presence_of_element_located((By.ID, "track_aorta_ENCFF115HTK")))

        track = self.driver.find_element(By.ID, 'track_aorta_ENCFF115HTK')

        action = ActionChains(self.driver)

        action.move_to_element_with_offset(track, start, 50).perform()

        action.click_and_hold().perform()

        action.move_by_offset(end - start, 50)

        action.release().perform()

        wait.until(EC.presence_of_element_located((By.ID, "track_aorta_ENCFF115HTK")))

    def zoomIn(self):
        element = self.driver.find_element(By.ID, 'bigZoomIn')

        element.click()

        time.sleep(2)

    def zoomOut(self):
        element = self.driver.find_element(By.ID, 'bigZoomOut')

        element.click()

        time.sleep(2)

    def locationCheck(self, location):
        track = self.driver.find_element(By.ID, 'track_aorta_ENCFF115HTK')

        action = ActionChains(self.driver)

        action.move_to_element_with_offset(track, location, 50).perform()

        action.context_click().perform()

        time.sleep(10)

    def tearDown(self) -> None:
        self.driver.close()