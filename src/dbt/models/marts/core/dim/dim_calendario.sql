select
    cal.DAT_INICIO_ATIVIDADE                                    as DAT_INICIO_ATIVIDADE,
    year(cal.DAT_INICIO_ATIVIDADE)                              as NUM_ANO,
    month(cal.DAT_INICIO_ATIVIDADE)                             as NUM_MES,
    monthname(cal.DAT_INICIO_ATIVIDADE)                         as NOM_MES,
    case
        when month(cal.DAT_INICIO_ATIVIDADE) in (1, 2, 3) then 1
        when month(cal.DAT_INICIO_ATIVIDADE) in (4, 5, 6) then 2
        when month(cal.DAT_INICIO_ATIVIDADE) in (7, 8, 9) then 3
        when month(cal.DAT_INICIO_ATIVIDADE) in (10, 11, 12) then 4
        else null
    end                                                         as NUM_TRIMESTRE,
    case
        when month(cal.DAT_INICIO_ATIVIDADE) in (1, 2, 3, 4, 5, 6) then 1
        when month(cal.DAT_INICIO_ATIVIDADE) in (7, 8, 9, 10, 11, 12) then 2
        else null
    end                                                         as NUM_SEMESTRE,
    case
        when dayofweek(cal.DAT_INICIO_ATIVIDADE) in (1, 7) then 1
        else 0
    end                                                         as FLG_FINAL_SEMANA
from {{ ref('stg_estabelecimento') }} cal