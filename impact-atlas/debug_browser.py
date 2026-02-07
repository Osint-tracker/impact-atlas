from playwright.sync_api import sync_playwright
import time
import os

def run():
    print("STARTING BROWSER AGENT...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True) # Run headless to not disturb user
        page = browser.new_page()
        
        # 1. Open Site
        print("Opening 127.0.0.1:8081...")
        page.goto("http://127.0.0.1:8081")
        time.sleep(5) # Wait for map init
        
        # 2. Toggle Units
        print("Toggling ORBAT Units...")
        # Find the checkbox. It has no ID but is inside label "ORBAT Units (Live)"
        # We use a robust selector finding the text then the input
        try:
            # Click the checkbox inside the label containing "ORBAT Units"
            page.locator("label:has-text('ORBAT Units') input[type='checkbox']").click()
            print("Toggle Clicked.")
        except Exception as e:
            print(f"Failed to find/click toggle: {e}")
            browser.close()
            return

        time.sleep(5) # Wait for fetch and render

        # 3. Check for Markers
        print("Searching for .unit-flag-marker...")
        markers = page.locator(".unit-flag-marker")
        count = markers.count()
        print(f"Found {count} markers on screen.")

        if count > 0:
            # 4. Click First Marker
            print("Clicking first marker...")
            try:
                # Force click in case of overlay
                markers.first.click(force=True)
                time.sleep(2)
                
                # 5. Check for Modal
                modal = page.locator("#unitModal")
                if modal.is_visible():
                    print("SUCCESS: Unit Modal is VISIBLE!")
                else:
                    print("FAILURE: Unit Modal is NOT visible after click.")
                    
            except Exception as e:
                print(f"Click failed: {e}")
        else:
             print("NO MARKERS FOUND. Verify CSS or Data.")

        # 6. Screenshot
        print("Saving screenshot to debug_browser_result.png")
        page.screenshot(path="debug_browser_result.png")
        
        browser.close()
        print("AGENT FINISHED.")

if __name__ == "__main__":
    run()
