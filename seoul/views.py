from django.shortcuts import render
from .tests import update

def home(request):
    update()
    return render(request, 'home.html')
