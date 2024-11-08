DATASETS = {
    # dataset: (exchange, [product types])
    'DBEQ.BASIC': (
        ('IEX TOPS', 'Equities'),
        ('MIAX Depth of Market', 'Equities'),
        ('NYSE Chicago Pillar', 'Equities'),
        ('NYSE National BBO + Trades', 'Equities'),
    ),
    'GLBX.MDP3': ('CME Globex MDP 3.0', ('Futures', 'Options')),
    'NDEX.IMPACT': ('ICE Endex iMpact', ('Futures', 'Options')),
    'IFEU.IMPACT': ('ICE Futures Europe iMpact', ('Futures', 'Options')),
    'OPRA.PILLAR': ('OPRA Pillar', 'Options'),
    'XNAS.ITCH': ('Nasdaq TotalView-ITCH', 'Equities'),
}
