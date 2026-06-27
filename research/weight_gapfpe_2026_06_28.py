# -*- coding: utf-8 -*-
"""사용자 아이디어: gap·fpe로 비중가중(하드컷 대신). 결론: LOWO 사후몰빵 착시로 기각, 하드게이트만 robust.
w∝gap LOWO=현행과 동일(+96/+14)=winner 빼면 무효. w∝gap/fpe LOWO 겨우 +3~6p. 하드게이트2.5 LOWO +56p robust.
구조: 가중은 들고있는 종목 못바꾸고 비중만→winner제외시 edge소멸. 하드게이트는 종목셋 바꿔서 robust.
재현 로직은 research/conviction_weight_2026_06_26.py + valuation_grid_2026_06_28.py 참조(동일 harness, mode만 추가).
"""
