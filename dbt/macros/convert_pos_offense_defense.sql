 {#
    This macro returns when a position is on offense or defense
#}

{% macro convert_pos_offense_defense(position) -%}

    case {{ position }}
        when 'LB' then 'DEF'
        when 'CB' then 'DEF'
        when 'G' then 'OFF'
        when 'WR' then 'OFF'
        when 'S' then 'DEF'
        when 'DT' then 'DEF'
        when 'DE' then 'DEF'
        when 'T' then 'OFF'
        when 'RB' then 'OFF'
        when 'TE' then 'OFF'
        when 'C' then 'OFF'
        when 'QB' then 'OFF'
        when 'K' then 'ST'
        when 'LS' then 'ST'
        when 'P' then 'ST'
        when 'KR' then 'ST'
        when 'FB' then 'OFF'
        when 'PR' then 'OFF'
    end


{%- endmacro %}