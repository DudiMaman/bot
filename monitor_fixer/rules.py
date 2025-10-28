def fix_bad_symbol(config_text: str, bad_symbols: list[str]) -> str:
    # מסיר סימבולים לא זמינים מה-config.yml
    out = config_text
    for sym in bad_symbols:
        out = out.replace(f'- "{sym}"', f'# removed_by_agent: {sym}')
    return out

def fix_config_key_alias(config_text: str) -> str:
    # דוגמה: שינוי שמות שדות בקונפיג (אם השתנו במחלקות)
    out = (config_text
           .replace("donchian_window:", "donchian_len:")
           .replace("adx_minimum:", "adx_min:")
           )
    return out
