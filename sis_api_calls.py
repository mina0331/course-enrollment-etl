import requests

def get_course_data():
    API_URL = "https://sisuva.admin.virginia.edu/psc/ihprd/UVSS/SA/s/WEBLIB_HCX_CM.H_CLASS_SEARCH.FieldFormula.IScript_ClassSearch?institution=UVA01&term=1262&acad_career=UGRD"
    #Getting the current semester's courses from the UVA SIS API for undergraduate students
    response = requests.get(API_URL)
    if response.status_code == 200:
    #send a GET request to the API endpoint for the current semester courses 
        course_data = response.json()  # Parse the JSON response
        return course_data
    else:
        # Handle the case where the request was not successful
        return None
    