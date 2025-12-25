
import requests


def pull_sis_api_to_raw_data(term):
    API_URL = f"https://sisuva.admin.virginia.edu/psc/ihprd/UVSS/SA/s/WEBLIB_HCX_CM.H_CLASS_SEARCH.FieldFormula.IScript_ClassSearch?institution=UVA01&term={term}&acad_career=UGRD"
    #Getting the current semester's courses from the UVA SIS API for undergraduate students
    response = requests.get(API_URL)
    if response.status_code == 200:
    #send a GET request to the API endpoint for the current semester courses 
        course_data = response.json()  # Parse the JSON response
        return course_data
    else:
        # Handle the case where the request was not successful
        return None


if __name__ == "__main__":
    pull_sis_api_to_raw_data()
    #allowing the script to be run directly not through import only

    
    
    