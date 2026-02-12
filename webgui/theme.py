"""
Theme configuration for multi-deployment support.
Reads SITE_THEME env var to select branding (svenskeb or bvpro).
"""

import os

THEMES = {
    'svenskeb': {
        'site_name': 'Svensk EnergiBesparing',
        'site_short_name': 'SvenskEB',
        'site_title_suffix': 'Heating System',
        'copyright_holder': 'Svensk EnergiBesparing',
        'email_prefix': 'Svenskeb',
        'email_system_name': 'Svenskeb Heating System',
        'subtitle': 'Heating System Management',
        'register_subtitle': 'Register for Svenskeb Heating System',
        'logo_filename': 'images/logo.png',
        'audit_app_name': 'SvenskebGUI',
        'login_url_display': 'https://svenskeb.se',
        'css_overrides': {},
    },
    'bvpro': {
        'site_name': 'BalansVärme Pro',
        'site_short_name': 'BVPro',
        'site_title_suffix': 'Building Energy Management',
        'copyright_holder': 'BalansVärme Pro',
        'email_prefix': 'BVPro',
        'email_system_name': 'BVPro Building Energy Management',
        'subtitle': 'Building Energy Management',
        'register_subtitle': 'Register for BVPro Energy Management',
        'logo_filename': 'images/themes/bvpro/logo.png',
        'audit_app_name': 'BVProGUI',
        'login_url_display': 'https://bvpro.hem.se',
        'css_overrides': {
            '--primary': '#1a7a4c',
            '--primary-dark': '#145e3a',
            '--dark': '#1e2d3d',
        },
    },
}


def get_theme(theme_name: str = None) -> dict:
    """Return theme dict for the given name, or from SITE_THEME env var.

    Falls back to 'svenskeb' if unset or unknown.
    """
    if theme_name is None:
        theme_name = os.environ.get('SITE_THEME', 'svenskeb')
    return THEMES.get(theme_name, THEMES['svenskeb'])
