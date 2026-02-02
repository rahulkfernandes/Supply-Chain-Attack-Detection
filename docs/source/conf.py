# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
import os
import sys
sys.path.insert(0, os.path.abspath('../..'))  # Points to Supply-Chain-Attack_Dectection/src/

project = 'Supply Chain Attack Detection'
copyright = '2026, Rahul Kenneth Fernandes'
author = 'Rahul Kenneth Fernandes'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',  # For Google or NumPy style docstrings
    'sphinx.ext.viewcode',  # Adds links to source code
    'sphinx_autodoc_typehints',  # If you installed it
    'myst_parser',  # If using Markdown
]

autodoc_default_options = {
    'members': True,  # Include all members (functions, classes, etc.)
    'undoc-members': True,  # Include undocumented members
    'private-members': False,  # Exclude private members (starting with _)
    'special-members': '__init__',  # Include special methods like __init__
    'show-inheritance': True,  # Show class inheritance
}

templates_path = ['_templates']
exclude_patterns = []



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
