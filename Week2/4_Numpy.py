# %%
import numpy as np

digits = np.array([[1,2,3], [4,5,6],[7,8,9]])
digits

curve_center = 80
grades = np.array([72, 35, 64, 88, 51, 90, 74, 12])
def curve(grades:np.ndarray):
    avg = grades.mean()
    change = curve_center - avg
    new_grades = grades + change
    return np.clip(new_grades, grades, 100)

curve(grades)