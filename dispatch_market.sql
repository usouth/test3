
--中间表
-- 调度中间表 bidata.will_department_list_crm
--依赖 dw_hf_mobdb.dw_sys_department;

insert overwrite table hf_mobdb.will_department_list_crm partition(d='analyse_date')
select current_timestamp update_time
    ,a.stats_date
    ,a.department_id 
    ,a.department_name
    ,size(split(a.dep_name_concat,','))as department_len 
    ,split(a.dep_name_concat,',')[0]as first_level
    ,0 as first_level_id
    ,split(a.dep_name_concat,',')[1]as second_level
    ,split(a.dep_id_concat,',')[0]as second_level_id

    ,split(a.dep_name_concat,',')[2]as third_level
    ,split(a.dep_id_concat,',')[1]as third_level_id

    ,split(a.dep_name_concat,',')[3]as fourth_level
    ,split(a.dep_id_concat,',')[2]as fourth_level_id

    ,split(a.dep_name_concat,',')[4]as fifth_level
    ,split(a.dep_id_concat,',')[3]as fifth_level_id

    ,split(a.dep_name_concat,',')[5]as sixth_level
    ,split(a.dep_id_concat,',')[4]as sixth_level_id


    ,split(a.dep_name_concat,',')[6]as seventh_level
    ,split(a.dep_id_concat,',')[5]as seventh_level_id

    ,split(a.dep_name_concat,',')[7]as eighth_level
    ,split(a.dep_id_concat,',')[6]as eighth_level_id
    ,a.city
    ,a.branch
    ,a.center
    ,a.region
    ,a.department
    ,a.grp
from(
    select date_sub(current_date,1) stats_date
        ,sd.department_name
        ,sd.department_id
        ,sd.pid
        ,case when locate('管培',sd.department_name) = 1  OR sd.department_name like '%CC%' or sd.department_name like '%CR%'
               then '上海' else substr(sd.department_name,1,2) end as city

        ,case when locate('销售',sd.department_name) >0 then '销售'
              when locate('学管',sd.department_name) >0 then '学管'else null end as branch

        ,case when sd.department_name like 'CC_大区%' then substr(sd.department_name,locate('CC',sd.department_name),locate('大区',sd.department_name)+1)
              when sd.department_name like 'CC考核区_部%' or sd.department_name='CC考核区待分配部' then 'CC考核区'
              when sd.department_name like '%CC入职培训部%' then 'CC入职培训部'
              when sd.department_name in('CC考核区','CC其他部门') then sd.department_name end as center

        ,case when sd.department_name like 'CC_大区%' then substr(sd.department_name,locate('大区',sd.department_name)+2,locate('区',sd.department_name,locate('大区',sd.department_name)+2)-locate('区',sd.department_name))
              else null end as region

        ,case when (sd.department_name like 'CC_大区%' or sd.department_name like 'CC考核区_部%') and sd.department_name not like 'CC_大区'  then substr(sd.department_name,locate('区',sd.department_name,locate('大区',sd.department_name)+2)+1)
              when sd.department_name='CC考核区待分配部' then '待分配部'
              when sd.department_name like '%部CR%组' then substr(sd.department_name,1,locate('部',sd.department_name))
              when sd.department_name like '%CR入职培训部' or sd.department_name like '上海__入职培训部' then '入职培训部'
              when sd.department_name like 'CR考核_部' or sd.department_name='CR培训部' then sd.department_name
              when  sd.department_name like '在线事业部CR%'  then '在线事业部CR'
              when sd.department_name like 'CR_部%' then substr(sd.department_name,3,locate('部',sd.department_name)-locate('CR',sd.department_name)-1)
              else null end as department

        ,case when sd.department_name like '%部CR%组' then substr(sd.department_name,locate('CR',sd.department_name))
              when locate('北京学管',sd.department_name) > 0  then substr(sd.department_name,locate('学管',sd.department_name)+2,locate('组',sd.department_name)-locate('学管',sd.department_name))
              when locate('组',sd.department_name) > 0 then substr(sd.department_name,locate('部',sd.department_name)+1,locate('组',sd.department_name)-locate('部',sd.department_name)+1) end as grp
       
        ,concat_ws(',',dsd8.department_name,dsd7.department_name,dsd6.department_name,dsd5.department_name,dsd4.department_name,dsd3.department_name
            ,dsd2.department_name,dsd1.department_name,sd.department_name)as dep_name_concat
       
        ,concat_ws(',',cast(dsd8.department_id as string),cast(dsd7.department_id as string),cast(dsd6.department_id as string),cast(dsd5.department_id as string),cast(dsd4.department_id as string)
            ,cast(dsd3.department_id as string),cast(dsd2.department_id as string),cast(dsd1.department_id as string),cast(sd.department_id as string))as dep_id_concat
    from dw_hf_mobdb.dw_sys_department sd
    left join(
        select department_name
            ,department_id
            ,pid
        from dw_hf_mobdb.dw_sys_department sd
        )dsd1 on sd.pid=dsd1.department_id

    left join(
        select department_name
            ,department_id
            ,pid
        from dw_hf_mobdb.dw_sys_department sd
        )dsd2 on dsd1.pid=dsd2.department_id
    left join(
        select department_name
            ,department_id
            ,pid
        from dw_hf_mobdb.dw_sys_department sd
        )dsd3 on dsd2.pid=dsd3.department_id
    left join(
        select department_name
            ,department_id
            ,pid
        from dw_hf_mobdb.dw_sys_department sd
        )dsd4 on dsd3.pid=dsd4.department_id
    left join(
        select department_name
            ,department_id
            ,pid
        from dw_hf_mobdb.dw_sys_department sd
        )dsd5 on dsd4.pid=dsd5.department_id
    left join(
        select department_name
            ,department_id
            ,pid
        from dw_hf_mobdb.dw_sys_department sd
        )dsd6 on dsd5.pid=dsd6.department_id
    left join(
        select department_name
            ,department_id
            ,pid
        from dw_hf_mobdb.dw_sys_department sd
        )dsd7 on dsd6.pid=dsd7.department_id
    left join(
        select department_name
            ,department_id
            ,pid
        from dw_hf_mobdb.dw_sys_department sd
        )dsd8 on dsd7.pid=dsd8.department_id
    )a
;








-- 调度中间表 bidata.will_user_list_crm
--依赖 dw_hf_mobdb.dw_view_user_info;dw_hf_mobdb.dw_view_communication_record;dw_hf_mobdb.dw_sys_user_role;
--dw_hf_mobdb.dw_sys_role;dw_hf_mobdb.dw_ddic;dw_hf_mobdb.dw_sys_department;hf_mobdb.will_department_list_crm;dw_hf_mobdb.dw_view_communication_record

insert overwrite table hf_mobdb.will_user_list_crm partition(d='analyse_date')

select current_date() stats_date
    ,dvui.user_id
    ,dvui.name
    
    ,dvui.job_number
    ,dvui.role_code
    ,dvui.role_name

    ,dvui.reg_date
    ,dvcr.on_board_date
    ,dvcr.first_commu_time
    ,dvcr.last_commu_time
    ,dvcr.depart_date

    ,dvui.department_id
    ,dvui.department_name
    ,dvui.department_len
    ,dvui.first_level
    ,dvui.second_level
    ,dvui.third_level
    ,dvui.fourth_level
    ,dvui.fifth_level
    ,dvui.sixth_level
    ,dvui.seventh_level
    ,dvui.eighth_level
    ,dvui.city
    ,dvui.branch
    ,dvui.center
    ,dvui.region
    ,dvui.department
    ,dvui.grp
    ,current_timestamp() update_time

from(
    select 
        ui.user_id,
        ui.name,
        ui.reg_date,
        ui.job_number,
        sr.role_code,
        dd.value as role_name, 
        sd.department_id,
        sd.department_name,
        dl.department_len,
        dl.first_level,
        dl.second_level,
        dl.third_level,
        dl.fourth_level,
        dl.fifth_level,
        dl.sixth_level,
        dl.seventh_level,
        dl.eighth_level
        ,case when locate('管培',sd.department_name) = 1  OR sd.department_name like '%CC%' or sd.department_name like '%CR%'
               then '上海' else substr(sd.department_name,1,2) end as city

        ,case when locate('销售',sd.department_name) >0 then '销售'
              when locate('学管',sd.department_name) >0 then '学管'else null end as branch

        ,case when sd.department_name like 'CC_大区%' then substr(sd.department_name,locate('CC',sd.department_name),locate('大区',sd.department_name)+1)
              when sd.department_name like 'CC考核区_部%' or sd.department_name='CC考核区待分配部' then 'CC考核区'
              when sd.department_name like '%CC入职培训部%' then 'CC入职培训部'
              when sd.department_name in('CC考核区','CC其他部门') then sd.department_name end as center

        ,case when sd.department_name like 'CC_大区%' then substr(sd.department_name,locate('大区',sd.department_name)+2,locate('区',sd.department_name,locate('大区',sd.department_name)+2)-locate('区',sd.department_name))
              else null end as region

        ,case when (sd.department_name like 'CC_大区%' or sd.department_name like 'CC考核区_部%') and sd.department_name not like 'CC_大区'  then substr(sd.department_name,locate('区',sd.department_name,locate('大区',sd.department_name)+2)+1)
              when sd.department_name='CC考核区待分配部' then '待分配部'
              when sd.department_name like '%部CR%组' then substr(sd.department_name,1,locate('部',sd.department_name))
              when sd.department_name like '%CR入职培训部' or sd.department_name like '上海__入职培训部' then '入职培训部'
              when sd.department_name like 'CR考核_部' or sd.department_name='CR培训部' then sd.department_name
              when  sd.department_name like '在线事业部CR%'  then '在线事业部CR'
              when sd.department_name like 'CR_部%' then substr(sd.department_name,3,locate('部',sd.department_name)-locate('CR',sd.department_name)-1)
              else null end as department

        ,case when sd.department_name like '%部CR%组' then substr(sd.department_name,locate('CR',sd.department_name))
              when locate('北京学管',sd.department_name) > 0  then substr(sd.department_name,locate('学管',sd.department_name)+2,locate('组',sd.department_name)-locate('学管',sd.department_name))
              when locate('组',sd.department_name) > 0 then substr(sd.department_name,locate('部',sd.department_name)+1,locate('组',sd.department_name)-locate('部',sd.department_name)+1) end as grp
        
    from dw_hf_mobdb.dw_view_user_info ui 

    left join dw_hf_mobdb.dw_sys_user_role sur on ui.user_id=sur.user_id
    left join dw_hf_mobdb.dw_sys_role sr on sur.role_id=sr.role_id
    left join dw_hf_mobdb.dw_ddic dd on sr.role_code=dd.description and dd.type='TP095'
    left join dw_hf_mobdb.dw_sys_department sd on sr.department_id=sd.department_id
    left join hf_mobdb.will_department_list_crm dl on dl.department_id = sd.department_id
    where sd.department_id is not null and ui.account_type = 1
    )dvui
left join(
    select 
        communication_person user_id,
        min(end_time) first_commu_time,
        max(end_time) last_commu_time,
        '' as on_board_date,
        '' as depart_date
    from dw_hf_mobdb.dw_view_communication_record
    -- where communication_person in {0}
    group by communication_person
    )dvcr on dvui.user_id=dvcr.user_id
;
  


-- 调度中间表 bidata.market_lesson_audio
insert overwrite table  hf_mobdb.market_lesson_audio partition (d='analyse_date')
select b.lesson_plan_id
    , order_id
    ,(case 
            when lesson_status = '5' then '教学质检取消' 
            when lesson_status= '4' then '取消'
            when lesson_status ='1'then '系统' 
            when lesson_status ='2'then '非系统'
            when lesson_status ='3'  then '尚未开始' 
            else '其他' 
        end) lesson_status 
    , adjust_start_time
    , adjust_end_time
    , start_month
    , end_month
    ,subject_name, exam_year, city, create_time, create_date, student_no, student_name
    ,teacher_name, ADID, origin, adid_account, plan, key_word, coil, submit_kf, student_province
    ,is_expand, is_first, follow_sales, timetable_date, s_phone7, apply_user_id, apply_user_name
    ,apply_user_department
    ,current_timestamp as update_time
    , cancel_time ,device,apply_time,schedule_type
from(
select lesson_plan_id
    , order_id
    , lesson_status
    , adjust_start_time
    , adjust_end_time
    , start_month
    , end_month
    ,subject_name, exam_year, city, create_time, create_date, student_no, student_name
    ,teacher_name, ADID, origin, adid_account, plan, key_word, coil, submit_kf, student_province
    ,is_expand, is_first, follow_sales, timetable_date, s_phone7, apply_user_id, apply_user_name
    ,apply_user_department, cancel_time, apply_time,device,schedule_type
    ,row_number() over(partition by a.student_no order by a.lesson_status) as rank
from(   
    select
        lp.lesson_plan_id,
        lpo.order_id,

        (case 
            when lp.solve_status = 6 then '5'
            when lp.status= 0 then '4'
            when lp.status =3 then '1' 
            when lp.status =5 then '2'
            when lp.status in (1,2,4,6)  then '3'
            else '6' 
        end) as lesson_status,

        lp.adjust_start_time,
        lp.adjust_end_time,
        regexp_replace(substr(lp.adjust_start_time,1,7),'-','')as start_month,
        regexp_replace(substr(lp.adjust_end_time,1,7),'-','')as end_month,
        su.subject_name,

        s.exam_year,
        substr(s.phone,1,7)as s_phone7,
        mpcc.regLoc as city,

        s.create_time,
        to_date(s.create_time) create_date,
        s.student_no,
        s.name student_name,

        adid.adid_no as ADID,
        dd.value as origin,
        taa.account as adid_account,
        tap.plan_name as plan,
        adid.originality as key_word,
        dd1.value as coil,
        ui.name as submit_kf,
        a.area_name as student_province,

        case when lp.adjust_start_time>lps.adjust_start_time then '是' else '否' end as is_expand,

        case when lp.adjust_start_time>lp1.adjust_start_time then '否' else '是' end as is_first,
        ui.name as follow_sales,

        lpo.apply_user_id,
        wulc.name as apply_user_name,
        wulc.department_name apply_user_department,
        zti.teacher_no,
        zti.teacher_name,
        zti.entry_time,
        zti.storage_time,
        zti.quarters quarters_type,
        tlph.opt_time as timetable_date,

        coalesce(hftlph.opt_time_max,hfoph.operate_time_max)as cancel_time,

        lpo.apply_time,
     
       case when opt_user=972038 then 'AI直排'
            when opt_user not in  (381322, 32547,972038) then '教务手动'  
            when opt_user=381322 and locate(hfvui.name,lpo.teacher_requirements) <> 0 then '指定老师'
            when opt_user=381322 and hflpo.order_id is not null then 'APP派单' 
            when opt_user is null then '未排课'  
            else '系统未知'  end as schedule_type, --#排课方式
        '' as device


    from dw_hf_mobdb.dw_lesson_plan lp --force index(idx_adjust_start_time)
    left join dw_hf_mobdb.dw_subject su on lp.subject_id=su.subject_id

    left join dw_hf_mobdb.dw_view_student s on lp.student_id = s.student_id

    left join dw_hf_mobdb.dw_map_phone_city_copy mpcc on substr(s.phone,1,7) = mpcc.phone7
    left join dw_hf_mobdb.dw_adid adid on adid.adid_id = s.adid_id
    left join dw_hf_mobdb.dw_ddic dd on s.know_origin=dd.code and dd.type='TP016'
    left join dw_hf_mobdb.dw_tms_adid_account taa on adid.account_code = taa.id
    left join dw_hf_mobdb.dw_tms_adid_plan tap on adid.plan_code = tap.id
    left join dw_hf_mobdb.dw_ddic dd1 on s.coil_in=dd1.code and dd1.type='TP023'
    left join dw_hf_mobdb.dw_view_user_info ui on s.submit_userid = ui.user_id
    left join dw_hf_mobdb.dw_area a on cast(substr(s.area_id,1,2) as int)*10000=a.id
    left join( 
        select student_id,min(adjust_start_time)as adjust_start_time
        from dw_hf_mobdb.dw_lesson_plan lp
        where lesson_type =1 
        and status in(3,5) 
        and solve_status<>6
        group by student_id
        )lps on s.student_id=lps.student_id

    left join(
        select student_id,min(adjust_start_time)as adjust_start_time
        from dw_hf_mobdb.dw_lesson_plan lp
        where lesson_type=2 
        and status in(3,5) 
        and solve_status<>6 
        group by student_id
        )lp1 on lp.student_id=lp1.student_id

    left join dw_hf_mobdb.dw_lesson_relation lr on lp.lesson_plan_id=lr.plan_id
    left join dw_hf_mobdb.dw_lesson_plan_order lpo on lr.order_id = lpo.order_id
    left join hf_mobdb.will_user_list_crm wulc on lpo.apply_user_id = wulc.user_id
    left join dw_hf_mobdb.dw_zm_teacher_info zti on zti.teacher_id = lp.teacher_id
    left join dw_hf_mobdb.dw_tms_lesson_plan_history tlph on tlph.lesson_plan_id = lp.lesson_plan_id and tlph.opt_type in (1,4)



    left join(
        select lesson_plan_id,max(opt_time) as opt_time_max
        from dw_hf_mobdb.dw_tms_lesson_plan_history 
        where opt_type = 2
        group by lesson_plan_id
        )hftlph  on hftlph.lesson_plan_id = lp.lesson_plan_id 

    left join(
        select oph.order_id,max(operate_time)as operate_time_max
        from dw_hf_mobdb.dw_order_operate_history oph 
        where oph.operate_type in (9,10)
        group by oph.order_id
        )hfoph on hfoph.order_id=lpo.order_id

    left join(
        select lpo2.order_id
        from dw_hf_mobdb.dw_lesson_plan_order  lpo2
        left join dw_hf_mobdb.dw_lesson_plan_schedule lps on lpo2.order_id = lps.order_id
        left join dw_hf_mobdb.dw_lesson_plan_schedule_detail lpsd on lpsd.schedule_id =  lps.id
        left join dw_hf_mobdb.dw_lesson_plan_schedule_detail_2018 lpsdd on lpsdd.schedule_id =  lps.id
        where (lpo2.inform_status = 5 or lpsd.teacher_status = 5 or lpsdd.teacher_status = 5) 
        )hflpo on hflpo.order_id = lpo.order_id

    left join(
        select user_id,name 
        from dw_hf_mobdb.dw_view_user_info vui
        )hfvui on hfvui.user_id = lp.teacher_id

    where wulc.stats_date = current_date
        and lp.lesson_type = 2
        and s.account_type = 1 --###去测试
        and lp.adjust_start_time >= '2019-04-01'
    -- order by field(lesson_status,'系统','非系统','尚未开始','取消','教学质检取消') --###对学生的课程进行排序
    order by lesson_status
    )a
)b
where b.rank=1
;






-- 调度中间表 bidata.market_lesson_audio_2
insert overwrite table  hf_mobdb.market_lesson_audio_2 partition (d='${analyse_date}')
select b.lesson_plan_id , order_id 
    , (case 
            when lesson_status = '5' then '教学质检取消' 
            when lesson_status= '4' then '取消'
            when lesson_status ='1'then '系统' 
            when lesson_status ='2'then '非系统'
            when lesson_status ='3'  then '尚未开始' 
            else '其他' 
        end) lesson_status 
    , adjust_start_time , adjust_end_time , start_month , end_month 
    , subject_name , exam_year , city , create_time , create_date , student_no , student_name , ADID 
    , origin , adid_account , plan , key_word , coil , submit_kf , student_province , is_expand , is_first 
    , follow_sales , timetable_date , s_phone7 , apply_user_id , apply_user_name , apply_user_department , teacher_no 
    , teacher_name , entry_time , storage_time , quarters_type 
    ,current_timestamp as update_time 
    , cancel_time , device 
    , apply_time 
from(
select lesson_plan_id , order_id , lesson_status , adjust_start_time , adjust_end_time , start_month , end_month 
    , subject_name , exam_year , city , create_time , create_date , student_no , student_name , ADID 
    , origin , adid_account , plan , key_word , coil , submit_kf , student_province , is_expand , is_first 
    , follow_sales , timetable_date , s_phone7 , apply_user_id , apply_user_name , apply_user_department , teacher_no 
    , teacher_name , entry_time , storage_time , quarters_type , cancel_time , device 
    , apply_time 
    ,row_number() over(partition by a.student_no,a.start_month order by a.lesson_status) as rank
from(   
    select
        lp.lesson_plan_id,
        lpo.order_id,

        (case 
            when lp.solve_status = 6 then '5'
            when lp.status= 0 then '4'
            when lp.status =3 then '1' 
            when lp.status =5 then '2'
            when lp.status in (1,2,4,6)  then '3'
            else '6' 
        end) as lesson_status,

        lp.adjust_start_time,
        lp.adjust_end_time,

        regexp_replace(substr(lp.adjust_start_time,1,7),'-','')as start_month,
        regexp_replace(substr(lp.adjust_end_time,1,7),'-','')as end_month,

        su.subject_name,

        s.exam_year,
        -- left(s.phone,7) s_phone7,
        substr(s.phone,1,7)as s_phone7,
        mpcc.regLoc as city,

        s.create_time,
        to_date(s.create_time) create_date,
        s.student_no,
        s.name student_name,

        adid.adid_no as ADID,
        dd.value as origin,
        taa.account as adid_account,
        tap.plan_name as plan,
        adid.originality as key_word,
        dd1.value as coil,
        ui.name as submit_kf,
        a.area_name as student_province,
        case when lp.adjust_start_time>lps.adjust_start_time then '是' else '否' end as is_expand,

        case when lp.adjust_start_time>lp1.adjust_start_time then '否' else '是' end as is_first,
        ui.name as follow_sales,

        lpo.apply_user_id,
        wulc.name as apply_user_name,
        wulc.department_name apply_user_department,

        zti.teacher_no,
        zti.teacher_name,
        zti.entry_time,
        zti.storage_time,
        zti.quarters quarters_type,

        tlph.opt_time as timetable_date,
        coalesce(hftlph.opt_time_max,hfoph.operate_time_max)as cancel_time,

        lpo.apply_time,    

       case when opt_user=972038 then 'AI直排'
            when opt_user not in  (381322, 32547,972038) then '教务手动'  
            when opt_user=381322 and locate(hfvui.name,lpo.teacher_requirements) <> 0 then '指定老师'
            when opt_user=381322 and hflpo.order_id is not null then 'APP派单' 
            when opt_user is null then '未排课'  
            else '系统未知'  end as schedule_type, --#排课方式
                   
        '' as device


    from dw_hf_mobdb.dw_lesson_plan lp --force index(idx_adjust_start_time)
    left join dw_hf_mobdb.dw_subject su on lp.subject_id=su.subject_id

    left join dw_hf_mobdb.dw_view_student s on lp.student_id = s.student_id

    left join dw_hf_mobdb.dw_map_phone_city_copy mpcc on substr(s.phone,1,7) = mpcc.phone7
    left join dw_hf_mobdb.dw_adid adid on adid.adid_id = s.adid_id
    left join dw_hf_mobdb.dw_ddic dd on s.know_origin=dd.code and dd.type='TP016'
    left join dw_hf_mobdb.dw_tms_adid_account taa on adid.account_code = taa.id
    left join dw_hf_mobdb.dw_tms_adid_plan tap on adid.plan_code = tap.id
    left join dw_hf_mobdb.dw_ddic dd1 on s.coil_in=dd1.code and dd1.type='TP023'
    left join dw_hf_mobdb.dw_view_user_info ui on s.submit_userid = ui.user_id
    left join dw_hf_mobdb.dw_area a on cast(substr(s.area_id,1,2) as int)*10000=a.id

    left join( 
        select student_id,min(adjust_start_time)as adjust_start_time
        from dw_hf_mobdb.dw_lesson_plan lp
        where lesson_type =1 
        and status in(3,5) 
        and solve_status<>6
        group by student_id
        )lps on s.student_id=lps.student_id

    left join(
        select student_id,min(adjust_start_time)as adjust_start_time
        from dw_hf_mobdb.dw_lesson_plan lp
        where lesson_type=2 
        and status in(3,5) 
        and solve_status<>6 
        group by student_id
        )lp1 on lp.student_id=lp1.student_id

    left join dw_hf_mobdb.dw_lesson_relation lr on lp.lesson_plan_id=lr.plan_id
    left join dw_hf_mobdb.dw_lesson_plan_order lpo on lr.order_id = lpo.order_id
    left join hf_mobdb.will_user_list_crm wulc on lpo.apply_user_id = wulc.user_id
    left join dw_hf_mobdb.dw_zm_teacher_info zti on zti.teacher_id = lp.teacher_id
    left join dw_hf_mobdb.dw_tms_lesson_plan_history tlph on tlph.lesson_plan_id = lp.lesson_plan_id and tlph.opt_type in (1,4)

    left join(
        select lesson_plan_id,max(opt_time) as opt_time_max
        from dw_hf_mobdb.dw_tms_lesson_plan_history 
        where opt_type = 2
        group by lesson_plan_id
        )hftlph  on hftlph.lesson_plan_id = lp.lesson_plan_id 

    left join(
        select oph.order_id,max(operate_time)as operate_time_max
        from dw_hf_mobdb.dw_order_operate_history oph 
        where oph.operate_type in (9,10)
        group by oph.order_id
        )hfoph on hfoph.order_id=lpo.order_id

    left join(
        select lpo2.order_id
        from dw_hf_mobdb.dw_lesson_plan_order  lpo2
        left join dw_hf_mobdb.dw_lesson_plan_schedule lps on lpo2.order_id = lps.order_id
        left join dw_hf_mobdb.dw_lesson_plan_schedule_detail lpsd on lpsd.schedule_id =  lps.id
        left join dw_hf_mobdb.dw_lesson_plan_schedule_detail_2018 lpsdd on lpsdd.schedule_id =  lps.id
        where (lpo2.inform_status = 5 or lpsd.teacher_status = 5 or lpsdd.teacher_status = 5) 
        )hflpo on hflpo.order_id = lpo.order_id

    left join(
        select user_id,name 
        from dw_hf_mobdb.dw_view_user_info vui
        )hfvui on hfvui.user_id = lp.teacher_id

    where wulc.stats_date = current_date
        and lp.lesson_type = 2
        and s.account_type = 1 --###去测试
        and lp.adjust_start_time >= '2019-04-01'
    order by lesson_status
    )a
)b
where b.rank=1
;


-- 调度中间表 bidata.shaobin_student_dimension
-- insert overwrite table hf_mobdb.shaobin_student_dimension
drop table if exists hf_mobdb.shaobin_student_dimension;
create table hf_mobdb.shaobin_student_dimension as 
select
    max(a.student_no) student_no,
    a.student_intention_id,
    max(a.channel) method,
    max(a.account) account,
    max(a.create_time) reg_time,
    max(a.frist_into_pool_date) data_first_assign_time,
    min(start_time)  first_call_time,
    min(bridge_time)  first_receive_time,
    max(a.frist_opt_time) first_arrange_shitingke_time,
    max(a.frist_adjust_start_time) first_shiting_start_time,
    max(a.first_adjust_end_time) first_shiting_end_time,
    max(a.first_adujust_start_time_real) first_pay_shitingke_start_time,
    max(a.first_adjust_end_time_real) first_pay_shitingke_end_time,
    max(a.submit_time) order_time,
    max(a.frist_formal_opt_time) first_arrange_zhengshike_time,
    max(a.frist_fomal_adjust_start_time) first_zhengshike_start_time,
    max(a.frist_fomal_adjust_end_time) first_zhengshike_end_time,
    max(a.frist_formal_ajust_start_time_real) first_pay_zhengshike_start_time,
    max(a.frist_fomal_adjust_end_time_real) first_pay_zhengshike_end_time,
    min(case when tcr.bridge_time >a.first_adjust_end_time_real then  bridge_time  end) after_first_shiting_first_receive_time,
    min(case when tcr.bridge_time >a.first_adjust_end_time_real then  start_time  end) after_first_shiting_first_call_time,


    min(case when tcr.bridge_time >a.first_adjust_end_time_real then(unix_timestamp(tcr.bridge_time) - unix_timestamp(a.first_adjust_end_time_real))/60/60 end)first_pay_shitingke_receive_interval_time,
    min(case when tcr.bridge_time >a.first_adjust_end_time_real then (unix_timestamp(start_time) -unix_timestamp(a.first_adjust_end_time_real))/60/60 end ) first_pay_shitingke_call_interval_time,

    min((unix_timestamp(a.frist_into_pool_date)-unix_timestamp(a.create_time))/60/60)reg_first_arrange_interval_time,
    min((unix_timestamp(tcr.start_time)-unix_timestamp(a.frist_into_pool_date))/60/60)arrange_call_first_interval_time,
    (unix_timestamp(min(tcr.bridge_time))-unix_timestamp(min(tcr.start_time)))/60/60 call_receive_first_interval_time,

    max((unix_timestamp( a.frist_opt_time) -unix_timestamp(a.frist_into_pool_date))/60/60/24)  assign_arrange_shitingke_first_interval_time,
    sum(case when tcr.start_time between a.frist_into_pool_date and a.frist_opt_time then 1 else 0 end)  first_assign_arrange_shitingke_call_count,
    sum(case when tcr.start_time between a.frist_into_pool_date and a.frist_opt_time and tcr.bridge_time is not null  then 1 else 0 end) first_assign_arrange_shitingke_receive_count,
    max(case when tcr.start_time between a.frist_into_pool_date and a.frist_opt_time then unix_timestamp(tcr.end_time) -unix_timestamp(tcr.bridge_time) end) first_assign_arrange_shitingke_longest_time,

    max((unix_timestamp(a.first_adujust_start_time_real)- unix_timestamp(a.frist_opt_time)) /60/60/24) first_arrange_pay_shitingke_interval_time,  
    sum(case when tcr.start_time between a.frist_opt_time and a.first_adujust_start_time_real then 1 else 0 end)  first_arrange_pay_shitingke_call_count,
    sum(case when tcr.start_time between a.frist_opt_time and a.first_adujust_start_time_real and tcr.bridge_time is not null  then 1 else 0 end) first_arrange_pay_shitingke_receive_count,
    max(case when tcr.start_time between a.frist_opt_time and a.first_adujust_start_time_real then unix_timestamp(tcr.end_time)-unix_timestamp(tcr.bridge_time) end) first_arrange_pay_shitingke_longest_time,

    max((unix_timestamp(a.submit_time) - unix_timestamp(a.frist_opt_time))/60/60/24)  first_arrange_order_shitingke_interval_time,
    sum(case when tcr.start_time between a.frist_opt_time and a.submit_time then 1 else 0 end)  first_arrange_order_shitingke_call_count,
    sum(case when tcr.start_time between a.frist_opt_time and a.submit_time and tcr.bridge_time is not null  then 1 else 0 end) first_arrange_order_shitingke_receive_count,
    max(case when tcr.start_time between a.frist_opt_time and a.submit_time then unix_timestamp(tcr.end_time) -unix_timestamp(tcr.bridge_time) end) first_arrange_order_shitingke_longest_time,

    max((unix_timestamp(a.frist_formal_opt_time)- unix_timestamp(a.submit_time)) /60/60/24)  first_arrange_order_zhengshike_interval_time,
    sum(case when tcr.start_time between a.submit_time and  a.frist_formal_opt_time then 1 else 0 end)  first_arrange_order_zhengshike_call_count,
    sum(case when tcr.start_time between a.submit_time and  a.frist_formal_opt_time and tcr.bridge_time is not null  then 1 else 0 end) first_arrange_order_zhengshike_receive_count,
    max(case when tcr.start_time between a.submit_time and a.frist_formal_opt_time then unix_timestamp(tcr.end_time)-unix_timestamp(tcr.bridge_time) end) first_arrange_order_zhengshike_longest_time,

    max((unix_timestamp(a.frist_formal_ajust_start_time_real)-unix_timestamp(a.submit_time ))/60/60/24)  first_pay_order_zhengshike_interval_time,
     sum(case when tcr.start_time between a.submit_time and  a.frist_formal_ajust_start_time_real then 1 else 0 end)  first_pay_order_zhengshike_call_count,
    sum(case when tcr.start_time between a.submit_time and  a.frist_formal_ajust_start_time_real and tcr.bridge_time is not null  then 1 else 0 end) first_pay_order_zhengshike_receive_count,
    max(case when tcr.start_time between a.submit_time and  a.frist_formal_ajust_start_time_real then unix_timestamp(tcr.end_time)-unix_timestamp(tcr.bridge_time)end) first_pay_order_zhengshike_longest_time,
  
    sum(case when tcr.start_time is not null then 1 else 0 end)  this_student_totall_call_time,
    sum(case when tcr.start_time is not null and tcr.bridge_time is not null  then 1 else 0 end) this_student_totall_receive_time,
    max(unix_timestamp(tcr.end_time) - unix_timestamp(tcr.bridge_time)) this_student_longest_time,
 
    '' as first_receive_arrange_shitingke_interval_time,

    current_timestamp as update_time


from(
    select s.student_intention_id,
        taa.account, --账户
        max(s.student_id) student_id,
        max(s.student_no) student_no,
        
        max(s.create_time) create_time,
        max(dd.value) channel, --渠道 

        min(into_pool_date) as frist_into_pool_date,--"数据首次分配时间",
        min(case when lp.lesson_type=2  then tlph.opt_time end)as frist_opt_time,-- "首次排试听课时间",
        min(case when lp.lesson_type=2  then lp.adjust_start_time end)as frist_adjust_start_time,--  "首次试听开始时间",
        min(case when lp.lesson_type=2  then lp.adjust_end_time end)as first_adjust_end_time,--  "首次试听结束时间",
        min(case when lp.lesson_type=2  and lp.status in(3,5) and lp.solve_status<>6  then lp.adjust_start_time end)as first_adujust_start_time_real,--  "首次消试听课开始时间",
        min(case when lp.lesson_type=2  and lp.status in(3,5) and lp.solve_status<>6  then lp.adjust_end_time end) as first_adjust_end_time_real,-- "首次消试听课结束时间",
        min(case when tc.status in(3,4,5,9) then tc.submit_time end)as submit_time,-- "成单登记时间",
        min(case when lp.lesson_type=1 and  lp.contract_id=tc.contract_id and tc.status in(3,4,5,9)  then tlph.opt_time end)as frist_formal_opt_time,-- "首次排正式课时间",
        min(case when lp.lesson_type=1 and  lp.contract_id=tc.contract_id and tc.status in(3,4,5,9)  then lp.adjust_start_time end)as frist_fomal_adjust_start_time,-- "首次正式课开始时间",
        min(case when lp.lesson_type=1 and  lp.contract_id=tc.contract_id and tc.status in(3,4,5,9)  then lp.adjust_end_time end)as frist_fomal_adjust_end_time,-- "首次正式课结束时间",            
        min(case when lp.lesson_type=1 and  lp.status in(3,5) and lp.solve_status<>6 and  lp.contract_id=tc.contract_id and tc.status in(3,4,5,9)  
            then lp.adjust_start_time end)as frist_formal_ajust_start_time_real,-- "首次消正式课开始时间",
        min(case when lp.lesson_type=1 and  lp.status in(3,5) and lp.solve_status<>6 and  lp.contract_id=tc.contract_id and tc.status in(3,4,5,9)  
            then lp.adjust_end_time end)as frist_fomal_adjust_end_time_real -- "首次消正式课结束时间"            

    from dw_hf_mobdb.dw_view_student s
    left join dw_hf_mobdb.dw_ddic dd on s.know_origin=dd.code and dd.type='TP016'
    left join dw_hf_mobdb.dw_adid adid on s.adid_Id=adid.adid_Id
    left join dw_hf_mobdb.dw_tms_adid_account taa on adid.account_code =taa.id

    left join(
        select intention_id,into_pool_date 
        from dw_hf_mobdb.dw_view_student s   
        left join dw_hf_mobdb.dw_tms_pool_exchange_log tpel  on s.student_intention_id=tpel.intention_id

        where s.create_time>=add_months(current_date,-2)
        and s.submit_time>=add_months(current_date,-2)

        and tpel.track_userid<>23251
        and s.name not like '%测试%'

        union all

        select s.student_intention_id,tfl.create_time 
        from dw_hf_mobdb.dw_view_student s   
        left join dw_hf_mobdb.dw_tms_new_name_get_log tfl on s.student_intention_id=tfl.student_intention_id
        where s.submit_time >=add_months(current_date,-2)
        and s.create_time >=add_months(current_date,-2)
        and s.name not like '%测试%'
        ) a on s.student_intention_id=a.intention_id
    left join dw_hf_mobdb.dw_lesson_plan lp on lp.student_id=s.student_id
    left join dw_hf_mobdb.dw_tms_lesson_plan_history tlph on tlph.lesson_plan_id=lp.lesson_plan_id
    left join dw_hf_mobdb.dw_view_tms_contract  tc  on s.student_intention_id=tc.student_intention_id

    where s.submit_time >=add_months(current_date,-2)
    and s.create_time >=add_months(current_date,-2)
    and s.name not like '%测试%'
    group by s.student_intention_id, taa.account
   ) a  
left  join dw_hf_mobdb.dw_view_tms_call_record tcr on a.student_intention_id=tcr.student_intention_id and end_time is not null     
group by a.student_intention_id
;











-- 调度中间表 bidata.student_submit_chenjia
drop table if exists hf_mobdb.student_submit_chenjia;
create table hf_mobdb.student_submit_chenjia as
select 
    s.create_time,  
    s.submit_time,  
    s.student_no,

    taa.account as adid_account,

    adid.adid_no as ADID,
    adid.originality as key_word, 
 
    tap.plan_name as plan,
    dd1.value as know_origin,
    dd2.value as coil_in,
    s.exam_year,
    case when mpc.regLoc is null then '其他' else mpc.regLoc end as city,
    case when ar.area_name is null then '其他' else ar.area_name end as province, 

    dd3.value intention_big_type,
    dd4.value earliest_intention, 

    (case 
        when (s.earliest_intention=9 and s.sale_stage2=0) or s.sale_stage2 in(1,3) then '新名单跟进阶段'
        when s.sale_stage2 in (6,9,10,11,7,8,12) then '试听阶段' 
        when s.sale_stage2 in(13,14) then '待成单阶段'
        when s.sale_stage2 =15 then '成单' 
        when s.sale_stage2 =2 then '无效' 
        when s.sale_stage2 = 4 then '未试听'
        when s.earliest_intention<>9 and s.sale_stage2 =0 then 'oc新名单' 
        else '其他' 
    end) stage,
    
    (case when s.earliest_intention <> 9 and s.sale_stage2 = 0 then dd5.value else dd6.value end)as stage_oc,
    ui.name,
    tb.communication_user_id,
    tb.communication_user_name,
    tb.communication_end_time,
    tb.communication_department_name,
    tb3.is_valid

from dw_hf_mobdb.dw_view_student s 
left join dw_hf_mobdb.dw_adid adid on s.adid_Id = adid.adid_Id 
left join dw_hf_mobdb.dw_tms_adid_account taa on adid.account_code = cast(taa.id as string)
left join dw_hf_mobdb.dw_tms_adid_plan tap on adid.plan_code = cast(tap.id as string)
left join dw_hf_mobdb.dw_ddic dd1 on s.know_origin=dd1.code and dd1.type='TP016'
left join dw_hf_mobdb.dw_ddic dd2 on s.coil_in=dd2.code and dd2.type='TP023' 
left join dw_hf_mobdb.dw_ddic dd3 on dd3.code = s.intention_big_type and dd3.type = 'TP046'
left join dw_hf_mobdb.dw_ddic dd4 on s.earliest_intention = dd4.code and dd4.type='TP065'


left join dw_hf_mobdb.dw_map_phone_city_copy mpc on substr(s.phone,1,7)=mpc.phone7
left join dw_hf_mobdb.dw_area ar on (mpc.provincenum*10000)=ar.id
left join dw_hf_mobdb.dw_view_user_info ui on s.track_userid=ui.user_id
left join 
    (select 
        a.student_intention_id,
        a.communication_person communication_user_id,
        ui.name communication_user_name,
        a.end_time communication_end_time,
        sd.department_name communication_department_name    
    from 
        (
        select 
                student_intention_id,

                row_number () OVER (PARTITION BY student_intention_id ORDER BY communication_record_id asc) AS rnk,
                communication_person,
                end_time
            from 
                dw_hf_mobdb.dw_view_communication_record 
            where role_code in ('XS-ZZ','XS-ZY','XS-JL','ZJ-ZZ','ZJ-ZY','ZJ-JL') 
        ) a 
    left join dw_hf_mobdb.dw_view_user_info ui on ui.user_id = a.communication_person
    left join dw_hf_mobdb.dw_sys_user_role sur on sur.user_id = a.communication_person
    left join dw_hf_mobdb.dw_sys_role sr on sr.role_id = sur.role_id
    left join dw_hf_mobdb.dw_sys_department sd on sd.department_id = sr.department_id
    where a.rnk = 1
    ) tb on tb.student_intention_id = s.student_intention_id

left join 
    (select
        tssh.student_intention_id,max(sd.sale_stage2) sale_stage2_max
    from 
        dw_hf_mobdb.dw_tms_sale_stage_history tssh
    left join dw_hf_mobdb.dw_view_student sd on tssh.student_intention_id=sd.student_intention_id
    group by tssh.student_intention_id
    ) t2 on t2.student_intention_id = s.student_intention_id

left join dw_hf_mobdb.dw_ddic dd5 on dd5.code = t2.sale_stage2_max and dd5.type='ST1111'
left join dw_hf_mobdb.dw_ddic dd6 on dd6.code=s.sale_stage2 and dd6.type='ST1111' 


left join 
    (
    select 
        a.student_intention_id,
        case when max(max_bridge_time)>=60 then '是' else '否' end is_valid
    from 
    (select
        sd.student_intention_id,
        max(unix_timestamp(tcr.end_time)-unix_timestamp(tcr.bridge_time)) max_bridge_time
    from 
        dw_hf_mobdb.dw_view_student sd 
    left join
        dw_hf_mobdb.dw_view_tms_call_record tcr on sd.student_intention_id=tcr.student_intention_id
    where sd.submit_time >= '2019-07-01' 
        and tcr.bridge_time < date_sub(sd.submit_time,-30)
        and tcr.bridge_time > sd.submit_time 
    group by sd.student_intention_id
    
    union --all
    
    select 
        sd.student_intention_id,
        max(calling_seconds) max_bridge_time
    from dw_hf_mobdb.dw_view_student sd 
    left join dw_hf_mobdb.dw_will_work_phone_call_recording wr on sd.student_intention_id = wr.student_intention_id
    where sd.submit_time>= '2019-07-01' 
    and wr.begin_time < date_sub(sd.submit_time,-30) 
    and wr.begin_time > sd.submit_time 
    group by sd.student_intention_id
    ) a 
    group by a.student_intention_id
    ) tb3 on tb3.student_intention_id = s.student_intention_id
left join dw_hf_mobdb.dw_data_charlie_oc dco on s.student_intention_id=dco.intention_id
where s.submit_time >= '2019-07-01' 
and dco.intention_id is null
;





-- 调度中间表 bidata.xiaozhen_2019_register
drop table if exists hf_mobdb.xiaozhen_2019_register;
create table hf_mobdb.xiaozhen_2019_register as 

select t1.create_time,t1.student_no,t1.student_name,t2.adid_account,t2.ADID,t2.key_word
    ,t2.coil,t2.plan,t2.know_origin,t1.exam_year,t2.city,t2.province,t2.intention_big_type
    ,t2.earliest_intention,t1.name,t2.stage,t3.stage_oc,t2.last_chage_adid_time,t1.which_week
    ,t1.llast_week,current_timestamp as update_time
from(
    select 
        s.create_time,
        s.student_no,
        s.name student_name,
        s.exam_year exam_year,
        ui.name name, 
        weekofyear(substr(s.create_time,1,19)) which_week,
        weekofyear(current_date) + 50 llast_week
        -- week(substr(s.create_time,1,19))+1 which_week,
        -- week(now()) + 51 llast_week
    from dw_hf_mobdb.dw_view_student s 
    left join dw_hf_mobdb.dw_view_user_info ui on s.track_userid=ui.user_id
    where substr(s.create_time,1,19) >= '2019-01-01'
    and substr(s.create_time,1,19) < current_date
    )t1
left join(
    select s.student_no,

        taa.account adid_account,
        adid.adid_no ADID,
        adid.originality key_word ,
        dd1.value coil,
        tap.plan_name plan,
        dd2.value know_origin,
        (case when mpc.regLoc is null then '其他' else mpc.regLoc end) city,
        (case when mpc.regLoc is null then '其他' else mpc.provinceName end) province,

        dd3.value intention_big_type,
        dd4.value earliest_intention,   

        (case when (s.earliest_intention=9 and s.sale_stage2=0) or s.sale_stage2 in(1,3) then '新名单跟进阶段'
            when  s.sale_stage2 in(6,9,10,11,7,8,12) then '试听阶段' when  s.sale_stage2 in(13,14) then '待成单阶段'
            when  s.sale_stage2 =15 then '成单'  when  s.sale_stage2 =2 then '无效' when  s.sale_stage2 =4 then '未试听'
            when s.earliest_intention<>9 and  s.sale_stage2 =0 then 'oc新名单' else '其他' end) stage,

        tacl.last_chage_adid_time
    from (
        select student_no, student_intention_id, adid_Id, coil_in, know_origin, intention_big_type, earliest_intention, sale_stage2, phone 
        from dw_hf_mobdb.dw_view_student
        where create_time >= '2019-01-01' 
        and create_time < current_date
        ) s 
    left join dw_hf_mobdb.dw_map_phone_city_copy mpc on substr(s.phone,1,7)=mpc.phone7
    left join dw_hf_mobdb.dw_adid adid on s.adid_Id = adid.adid_Id 
    left join dw_hf_mobdb.dw_tms_adid_account taa on adid.account_code = taa.id
    left join dw_hf_mobdb.dw_ddic dd1 on s.coil_in=dd1.code and dd1.type='TP023' 
    left join dw_hf_mobdb.dw_tms_adid_plan tap on adid.plan_code = tap.id
    left join dw_hf_mobdb.dw_ddic dd2 on s.know_origin=dd2.code and dd2.type='TP016' 
    left join dw_hf_mobdb.dw_ddic dd3 on dd3.code = s.intention_big_type and dd3.type = 'TP046'
    left join dw_hf_mobdb.dw_ddic dd4 on s.earliest_intention=dd4.code and dd4.type='TP065' 
    left join(
        select student_intention_id,max(concat(opt_date,' ',opt_time))as last_chage_adid_time
        from dw_hf_mobdb.dw_tms_adid_change_log
        where opt_user='系统'
        and adid_Id <> adid_id_old
        group by student_intention_id
        )tacl on tacl.student_intention_id = s.student_intention_id
    )t2 on t1.student_no=t2.student_no
left join(
    select s.student_no,
        (case when s.earliest_intention<>9 and  s.sale_stage2 =0 then dd1.value else dd2.value end )stage_oc
    from(
        select student_no, student_intention_id, earliest_intention, sale_stage2 
        from dw_hf_mobdb.dw_view_student 
        where create_time >= '2019-01-01'
        and create_time < current_date
        ) s 
    left join(
        select student_intention_id,max(sale_stage2) sale_stage2 
        from dw_hf_mobdb.dw_tms_sale_stage_history 
        group by student_intention_id
        ) x on x.student_intention_id=s.student_intention_id
    left join dw_hf_mobdb.dw_ddic dd1 on dd1.code=x.sale_stage2 and dd1.type='ST1111' 
    left join dw_hf_mobdb.dw_ddic dd2 on dd2.code=s.sale_stage2 and dd2.type='ST1111'
    )t3 on t1.student_no=t3.student_no
;






-- 调度中间表 bidata.jw_market_honghei_commu
drop table if exists hf_mobdb.jw_market_honghei_commu;
create table hf_mobdb.jw_market_honghei_commu as

select sql0.stats_date,sql0.user_id,sql0.name,sql0.job_number,sql0.department_name,sql1.call_time_total
    ,sql1.call_con_count_total,sql1.call_count_total,sql2.phone_call_cnt,sql2.phone_bridge_cnt,sql2.phone_seconds
    ,'' as class_period,sql3.lp_cnt,sql4.attend_time,current_timestamp as update_time
from(
    select 
        user_id,
        date_sub(stats_date,1) stats_date ,
        -- (stats_date -interval 1 day) stats_date ,
        department_name,
        name,
        job_number
    from hf_mobdb.will_user_list_crm 
    where stats_date >= date_sub(current_date,30) --curdate() - INTERVAL 30 day
    and (third_level = '市场城市化业务部' or (third_level = '代理-市场渠道合作中心' and fourth_level = '城市地推'))
    )sql0
left join(
    select 
        user_id,
        to_date(end_time) stats_date, --#拨打时间
        coalesce(sum(unix_timestamp(end_time)-unix_timestamp(bridge_time)),0) call_time_total,   --#'总通时',
        sum(case when bridge_time>end_time then 1 else 0 end) call_con_count_total,  --#'总接通次数',
        --  ifnull(sum(timestampdiff(second,bridge_time,end_time)),0) call_time_total,#'总通时',
        -- sum(case when timestampdiff(second,bridge_time,end_time) > 0 then 1 else 0 end) call_con_count_total,#'总接通次数',
        count(*) call_count_total   --#'总拨打次数',
    from dw_hf_mobdb.dw_view_tms_call_record vtcr
    where end_time >= date_sub(current_date,31) --date_sub(curdate(),interval 31 day) 
    and end_time < current_date   --curdate()   #限定为近30天
    and call_type =1  --#限定为打给学生
    group by user_id,to_date(end_time)
    )sql1 on sql1.user_id=sql0.user_id and sql1.stats_date=sql0.stats_date
left join(
    select
        a.user_id,
        a.stats_date,
        sum(phone_call_cnt) phone_call_cnt,
        sum(phone_seconds) phone_seconds,
        sum(phone_bridge_cnt) phone_bridge_cnt
    from 
        (select 
            user_id,
            to_date(begin_time) stats_date, -- 工作手机拨打时间
            count(id) phone_call_cnt, -- 工作手机通次
            sum(calling_seconds) phone_seconds, -- 工作手机通时
            sum(on_type) phone_bridge_cnt -- 工作手机接通通次
        from dw_hf_mobdb.dw_will_work_phone_call_recording
        where supplier='hujing'
        and begin_time >= date_sub(current_date,31) --curdate() - interval 31 day
        and begin_time < current_date --curdate()
        group by user_id,to_date(begin_time)

        union all
        select 
                user_id,
                to_date(begin_time) stats_date, -- 工作手机拨打时间
                count(id) phone_call_cnt, -- 工作手机通次
                sum(case when calling_seconds > 27.5 then calling_seconds-27.5 else 0 end) phone_seconds, -- 工作手机通时
                sum(case when calling_seconds > 27.5 then 1 else 0 end) phone_bridge_cnt --工作手机接通通次
        from dw_hf_mobdb.dw_will_work_phone_call_recording
        where supplier='aike'
        and begin_time >=  date_sub(current_date,31)
        and begin_time < current_date
        group by user_id,to_date(begin_time)
        ) a 
    group by a.user_id,a.stats_date
    )sql2 on sql2.user_id=sql0.user_id and sql2.stats_date=sql0.stats_date

left join(
    select stats_date,user_id
        -- ,sum(interval_x)
        ,count(lesson_plan_id)as lp_cnt
    from(
        select t1.*
            -- ,t2.real_end_time
            ,substr(t1.real_start_time,1,10)as stats_date
            -- ,round((unix_timestamp(t1.real_end_time)- uxix_timestamp(t1.real_start_time))/ 60, 2)as real_period 
            -- ,case when substr(real_real_end)<>substr(real_start_time) and (unix_timestamp(t1.real_end_time)- uxix_timestamp(t1.real_start_time))/ 60>35 then 5
            --     else (unix_timestamp(t1.real_end_time)- uxix_timestamp(t1.real_start_time))/ 60 end as interval_x
        from(
            select  
                tlph.opt_user user_id,
                lp.lesson_plan_id,
                real_start_time,
                real_end_time real_real_end
            from dw_hf_mobdb.dw_lesson_plan lp
            left join dw_hf_mobdb.dw_view_student s on s.student_id = lp.student_id
            left join dw_hf_mobdb.dw_tms_lesson_plan_history tlph on tlph.lesson_plan_id = lp.lesson_plan_id and tlph.opt_type = 1
            where s.account_type =1
            -- and tlph.opt_user in {0}
            and lp.lesson_type = 3  
            and lp.adjust_start_time >= date_sub(current_date,31) --date_sub(curdate(),interval 31 day)
            and lp.adjust_start_time < current_date 
            and lp.real_start_time is not null
            and lp.status = 3 
            and lp.solve_status <> 6
            )t1
        )b
    group by stats_date,user_id
    )sql3 on sql3.user_id=sql0.user_id and sql3.stats_date=sql0.stats_date

left join(
    select 
        distinct to_date(date_time) stats_date,
        user_id,
        date_time attend_time
    from dw_hf_mobdb.dw_sale_list_distribute_detail
    where date_time >=date_sub(current_date,31)
    and date_time < current_date
    )sql4 on sql4.user_id=sql0.user_id and sql4.stats_date=sql0.stats_date
;




-- 调度中间表  bidata.yxy_oc_get
insert overwrite table hf_mobdb.yxy_oc_get partition(d='analyse_date')
select
    to_date(tpel.into_pool_date) stat_date,
    tpel.into_pool_date,
    a.user_id,
    a.name,
    a.job_number,
    a.department_name,
    ui.name opt_user,
    a.city,a.branch,a.center,a.region,a.department,a.grp,
    s.student_no,
    s.student_intention_id,
    current_timestamp as update_time 
from dw_hf_mobdb.dw_tms_pool_exchange_log tpel
left join dw_hf_mobdb.dw_view_student s on s.student_intention_id = tpel.intention_id
left join 
    (select cd.user_id,cd.department_name,cd.name,cd.job_number,cd.stats_date,
            cd.city,cd.branch,cd.center,cd.region,cd.department,cd.grp
         from dt_mobdb.dt_charlie_dept_history cd
    ) a on a.user_id = tpel.track_userid and to_date(tpel.into_pool_date) = a.stats_date
left join dw_hf_mobdb.dw_view_user_info ui on ui.user_id = tpel.create_userid

left join(
    select tl1.track_userid,tl1.intention_id
    ,row_number() over (partition by tl1.intention_id order by tl1.id desc ) as rank
    from  dw_hf_mobdb.dw_tms_pool_exchange_log tl1
    left join dw_hf_mobdb.dw_tms_pool_exchange_log tl2 on tl1.intention_id=tl2.intention_id
    where tl1.id <tl2.id
    )tl on tl.intention_id=tpel.intention_id
left join dw_hf_mobdb.dw_view_user_info vui on tl.track_userid=vui.user_id

where tpel.into_pool_date  >=date_sub(current_date,1) 
and  tpel.into_pool_date <current_date
and a.department_name not like '%学管%'
  
and tl.rank=1
and vui.name like '%OC%'    
order by tpel.into_pool_date
;



-- 调度中间表 bidata.jw_sales_honghei_commu
-- ods.ls_classroom_origin缺失
drop table if exists hf_mobdb.jw_sales_honghei_commu;
create table hf_mobdb.jw_sales_honghei_commu as 

select a0.stats_date,a0.user_id,a0.name,a0.job_number,a0.tag,a1.call_time_total,a1.call_con_count_total
    ,a1.call_count_total    --天润拨打次数,
    ,a2.phone_call_cnt  --工作手机通次,
    ,a2.phone_bridge_cnt  --工作手机接通通次,
    ,a2.phone_seconds   --工作手机通时,
    ,'' as class_period    --体验课时长(分钟),
    ,a3.lp_cnt  --体验课数量,
    ,a4.attend_time  --打卡时间,
    ,a5.call_but_not_bridge_stu --天润外呼未接通非OC学生拨打人次（从未接通）,
    ,a5.call_not_bridge_phone_recall_stu --'天润外呼未接通非OC学生工作手机再拨打人次（从未接通）',
    ,a6.tcr_stu --'天润外呼未接通非OC线索数（从未接通）',
    ,a6.tcr_stu_ph --'天润外呼未接通非OC工作手机再拨线索数（从未接通）',
    ,'' as sale_commu_class_cnt --销售进入课堂沟通量,ls_sales_class_record a8
    ,'' as sale_in_lesson_cnt --销售进入课堂量, ods.ls_classroom_origin a7
    ,'' as timelength --销售进入课堂沟通时长ls_sales_class_record a8
    --销售进入课堂数
    ,current_timestamp as update_time 
    
from(
    select 
        user_id,
        date_sub(stats_date,1) stats_date,
        -- (stats_date - interval 1 day) stats_date,
        name,
        job_number,
        department_name,
        case when department_name like '%学管%' then '学管' else '销售' end tag
    from hf_mobdb.will_user_list_crm  wulc
    where (department_name like '%上海升学中心升学组%'
    or (wulc.first_level='客户关系部' and wulc.third_level in ("上海销售管理中心","全国学员服务中心","江苏分公司","销售管理中心",
        "学管培训管理中心","北京分公司","昆山分公司","上海销售项目中心","代理学管-渠道合作部")
    and wulc.department_name not like '%离职%'))
    and stats_date >= add_months(date_format(current_date(),'yyyy-MM-02'),-1)
    )a0

left join(
    select 
        user_id,
        to_date(tcr.end_time) stats_date, -- 拨打时间
        coalesce(sum(unix_timestamp(substr(tcr.end_time,1,19))-unix_timestamp(substr(tcr.bridge_time,1,19))) ,0) call_time_total,-- '总通时',
        sum(case when tcr.end_time>tcr.bridge_time then 1 else 0 end) call_con_count_total,-- '总接通次数',
        -- to_date(cast(substr(tcr.end_time,1,19) as timestamp)) stats_date, -- 拨打时间
        -- coalesce(sum(date_diff('second',cast(substr(tcr.bridge_time,1,19) as timestamp),cast(substr(tcr.end_time,1,19) as timestamp))),0) call_time_total,-- '总通时',
        -- sum(case when date_diff('second',cast(substr(tcr.bridge_time,1,19) as timestamp),cast(substr(tcr.end_time,1,19) as timestamp)) > 0 then 1 else 0 end) call_con_count_total,-- '总接通次数',
        count(*) call_count_total -- '总拨打次数',
    from dw_hf_mobdb.dw_view_tms_call_record tcr
    where substr(tcr.end_time,1,19) >= add_months(date_format(current_date(),'yyyy-MM-01'),-1)
    and substr(tcr.end_time,1,19) < current_date 
    -- where cast(substr(tcr.end_time,1,19) as timestamp) >= cast(date_format(current_date - interval '1' month,'%Y-%m-01') as TIMESTAMP) 
    -- and cast(substr(tcr.end_time,1,19) as timestamp) < current_date 
    and call_type =1  -- 限定为打给学生
    group by user_id,to_date(tcr.end_time)
    )a1 on a0.stats_date=a1.stats_date and a0.user_id=a1.user_id
left join(
    select
        a.user_id,
        a.stats_date,
        sum(phone_call_cnt) phone_call_cnt,
        sum(phone_seconds) phone_seconds,
        sum(phone_bridge_cnt) phone_bridge_cnt
    from 
            (select 
                user_id,
                to_date(begin_time) stats_date, -- 工作手机拨打时间
                count(id) phone_call_cnt, -- 工作手机通次
                sum(calling_seconds) phone_seconds, -- 工作手机通时
                sum(on_type) phone_bridge_cnt -- 工作手机接通通次
            from dw_hf_mobdb.dw_will_work_phone_call_recording
            where supplier='hujing'
            and begin_time >=  add_months(date_format(current_date(),'yyyy-MM-01'),-1)
            and begin_time < current_date()
            group by user_id,to_date(begin_time)

            union --all
            select 
                    user_id,
                    to_date(begin_time) stats_date, -- 工作手机拨打时间
                    count(id) phone_call_cnt, -- 工作手机通次
                    sum(case when calling_seconds > 27.5 then calling_seconds-27.5 else 0 end) phone_seconds, -- 工作手机通时
                    sum(case when calling_seconds > 27.5 then 1 else 0 end) phone_bridge_cnt --#工作手机接通通次
            from dw_hf_mobdb.dw_will_work_phone_call_recording
            where supplier='aike'
            and begin_time >= add_months(date_format(current_date(),'yyyy-MM-01'),-1)
            and begin_time < current_date()
            group by user_id,to_date(begin_time)
            ) a 
    group by a.user_id,a.stats_date
    )a2 on a0.stats_date=a2.stats_date and a0.user_id=a2.user_id

left join(
    select stats_date,user_id
        ,count(lesson_plan_id)as lp_cnt
    from(
        select t1.*
            ,substr(t1.real_start_time,1,10)as stats_date
        from(
            select  
                tlph.opt_user user_id,
                lp.lesson_plan_id,
                real_start_time,
                real_end_time real_real_end
            from dw_hf_mobdb.dw_lesson_plan lp
            left join dw_hf_mobdb.dw_view_student s on s.student_id = lp.student_id
            left join dw_hf_mobdb.dw_tms_lesson_plan_history tlph on tlph.lesson_plan_id = lp.lesson_plan_id and tlph.opt_type = 1
            where s.account_type =1
            -- and tlph.opt_user in {0}
            and lp.lesson_type = 3  
            and lp.adjust_start_time >= date_sub(current_date,31) --date_sub(curdate(),interval 31 day)
            and lp.adjust_start_time < current_date 
            and lp.real_start_time is not null
            and lp.status = 3 
            and lp.solve_status <> 6
            )t1
        )b
    group by stats_date,user_id
    )a3 on a0.stats_date=a3.stats_date and a0.user_id=a3.user_id

left join(
    select 
        distinct 
        to_date(date_time) stats_date,
        user_id,
        date_time attend_time

    from dw_hf_mobdb.dw_sale_list_distribute_detail
    where date_time >=add_months(date_format(current_date(),'yyyy-MM-01'),-1)
    and date_time < current_date()
    )a4 on a0.stats_date=a4.stats_date and a0.user_id=a4.user_id

left join(
    select 
        c.user_id,
        c.stats_date,
        count(c.student_intention_id) call_but_not_bridge_stu,--天润外呼未接通非oc人次,
        sum(case when c.phone_call_cnt>0 then 1 else 0 end) call_not_bridge_phone_recall_stu --天润未接通手机拨打人次
    from(
        select 
            a.stats_date,
            a.user_id,
            a.student_intention_id,
            b.phone_call_cnt
        from(
            select 
                tcr.user_id,
                tcr.student_intention_id,
                to_date(substr(tcr.end_time,1,19)) stats_date, --拨打时间
                sum(case when tcr.status = 33 then 1 else 0 end) as call_con_count_total --'总接通次数',
                
            from dw_hf_mobdb.dw_view_tms_call_record tcr

            left join hf_mobdb.yxy_oc_get yog on yog.student_intention_id =tcr.student_intention_id
            left join dw_hf_mobdb.dw_view_tms_call_record vtcr on vtcr.student_intention_id = tcr.student_intention_id  and vtcr.status = 33

            where substr(tcr.end_time,1,19)>= add_months(date_format(current_date(),'yyyy-MM-01'),-1) 
            and substr(tcr.end_time,1,19) < current_date
            and tcr.call_type =1  --限定为打给学生
            and yog.student_intention_id is null --非oc学生

            and substr(tcr.end_time,1,19) >= substr(tcr.end_time,1,19)
            group by tcr.user_id,tcr.student_intention_id, to_date(substr(tcr.end_time,1,19))
            having sum(case when tcr.status = 33 then 1 else 0 end) = 0  
            )a  --天润拨打未接通非oc学生
    left join 
        (
        select 
            user_id,
            to_date(begin_time) stats_date, --工作手机拨打时间
            student_intention_id,
            count(id) phone_call_cnt --工作手机通次
        from dw_hf_mobdb.dw_will_work_phone_call_recording
        where substr(begin_time,1,19)>=add_months(date_format(current_date(),'yyyy-MM-01'),-1)
        and substr(begin_time,1,19) < current_date
        group by user_id,to_date(begin_time),student_intention_id
        )b on b.user_id = a.user_id and b.stats_date = a.stats_date and b.student_intention_id = a.student_intention_id
        --同一个销售当天用天润拨打未接通学生用工作手机的拨打情况
    )c
    group by c.user_id,c.stats_date
    )a5 on a0.stats_date=a5.stats_date and a0.user_id=a5.user_id

left join(
    select 
        b.user_id,
        b.stats_date,
        count(b.student_intention_id)  tcr_stu,
        sum(case when c.phone_call_cnt>0 then 1 else 0 end) tcr_stu_ph

    from 
        (select
            to_date(end_time) stats_date,
            student_intention_id,
            user_id
        from 
            (select 
                cast(substr(tcr.end_time,1,19) as timestamp) end_time,
                tcr.student_intention_id,
                tcr.user_id,
                row_number() over (partition by tcr.student_intention_id order by substr(tcr.end_time,1,19) desc) as num
            from dw_hf_mobdb.dw_view_tms_call_record tcr
            left join dw_hf_mobdb.dw_view_student s on s.student_intention_id = tcr.student_intention_id
            left join hf_mobdb.yxy_oc_get yog on tcr.student_intention_id=yog.student_intention_id
            left join dw_hf_mobdb.dw_view_tms_call_record  vtcr on tcr.student_intention_id=vtcr.student_intention_id and  vtcr.status = 33

            where s.account_type =1
            and substr(tcr.end_time,1,19)  >= add_months(date_format(current_date(),'yyyy-MM-01'),-1)
            and substr(tcr.end_time,1,19) < current_date
            and tcr.student_intention_id <> 0
            and yog.student_intention_id is null
            and vtcr.student_intention_id is null
            ) a 
        where a.num =1
        )b 
    left join 
        (
        select 
            user_id,
            to_date(substr(begin_time,1,19)) stats_date,
            student_intention_id,
            count(id) phone_call_cnt -- 工作手机通次
        from dw_hf_mobdb.dw_will_work_phone_call_recording
        where substr(begin_time,1,19) >= add_months(date_format(current_date(),'yyyy-MM-01'),-1)
        and substr(begin_time,1,19)< current_date
        group by user_id,to_date(substr(begin_time,1,19)),student_intention_id
        )c on c.user_id = b.user_id and c.stats_date = b.stats_date and c.student_intention_id = b.student_intention_id
    group by b.user_id,b.stats_date
    )a6 on a0.stats_date=a6.stats_date and a0.user_id=a6.user_id
;


--数据集		3.4.1、2019市场试听数据_bidata
drop table if exists hf_mobdb.market_lesson_audio_bidata_2019;
create table hf_mobdb.market_lesson_audio_bidata_2019 as 
select k.*,
     case when k.lesson_status='取消' then 
        (case when  k.time_interval is null then null when k.time_interval >18 then '18小时以上' when k.time_interval >12 and k.time_interval <= 18 then '12至18小时以内'
        when k.time_interval >=6 and k.time_interval <= 18 then '6至18小时以内' when k.time_interval  <= 6 and k.time_interval >=0  then '6小时以内' when k.time_interval < 0 then '课后取消' end)
      when k.lesson_status='教学质检取消' then '课后取消' end as lesson_cancel_time_dist -- 课程取消时间分布
from
    (
    select
        mla.*,
        (case when lp.student_id is not null then '是' else '否' end) as if_experience,

        -- (case when (select count(*) from lesson_plan where student_id = s.student_id and lesson_type =3 
        --      and status in (3,5) and solve_status <> 6)>0 then '是' else '否' end) '是否上过体验课',


        if(hour(mla.adjust_start_time)<9,9,if(hour(mla.adjust_start_time)>=22,22,hour(mla.adjust_start_time))) - if(hour(mla.cancel_time)<9,9,if(hour(mla.cancel_time)>=22,22,
            hour(mla.cancel_time)))+minute(mla.adjust_start_time)/60 
            - minute(mla.cancel_time)/60 + datediff(mla.adjust_start_time,mla.cancel_time)*13 as time_interval, --'间隔'

        hfvs.track_userid,
        hfvs.track_user_name,
        hfvs.track_job_number
        -- hfvs.into_pool_date

        -- (select track_userid from tms_pool_exchange_log 
        --  where track_userid not in (select user_id from bidata.jw_abnormal_user)
        --  and intention_id = s.student_intention_id and into_pool_date < adjust_start_time order by into_pool_date desc limit 1
        -- ) track_userid,

        -- (select ui.name from view_user_info ui left join tms_pool_exchange_log tp on tp.track_userid = ui.user_id
        --  where track_userid not in (select user_id from bidata.jw_abnormal_user)
        --  and intention_id = s.student_intention_id and into_pool_date < adjust_start_time order by into_pool_date desc limit 1
        -- ) track_user_name,

        -- (select ui.job_number from view_user_info ui left join tms_pool_exchange_log tp on tp.track_userid = ui.user_id
        --  where track_userid not in (select user_id from bidata.jw_abnormal_user) and intention_id = s.student_intention_id
        --  and into_pool_date < adjust_start_time order by into_pool_date desc limit 1
        -- ) track_job_number
        
    FROM hf_mobdb.market_lesson_audio mla
    
    left join dw_hf_mobdb.dw_view_student s on s.student_no = mla.student_no 
    left join  dw_hf_mobdb.dw_lesson_plan lp on lp.student_id = s.student_id and lp.lesson_type =3 and lp.status in (3,5) and lp.solve_status <> 6

    left join(
        select tpel.intention_id,
            tpel.track_userid ,
            ui.name as track_user_name,
            ui.job_number as track_job_number,
            tpel.into_pool_date as into_pool_date,
            row_number() over (partition by tpel.track_userid order by tpel.into_pool_date desc )as rank
        from dw_hf_mobdb.dw_tms_pool_exchange_log tpel 
        left join  dw_hf_mobdb.dw_jw_abnormal_user jau on tpel.track_userid=jau.user_id
        left join  dw_hf_mobdb.dw_view_user_info ui on tpel.track_userid=ui.user_id
        where jau.user_id is null 
        )hfvs on hfvs.intention_id = s.student_intention_id

where mla.adjust_start_time>='2019-01-01'
and hfvs.into_pool_date<mla.adjust_start_time
and hfvs.rank=1
)k
;



--数据集       3.4.2、2018试听数据_每月去重

drop table if exists hf_mobdb.lesson_audio_mon_duplicate_2018;
create table hf_mobdb.lesson_audio_mon_duplicate_2018 as 

select k.*,
    case when k.lesson_status='取消' then 
        (case when  k.time_interval is null then null when k.time_interval >18 then '18小时以上' when k.time_interval >12 and k.time_interval <= 18 then '12至18小时以内'
        when k.time_interval >=6 and k.time_interval <= 18 then '6至18小时以内' when k.time_interval  <= 6 and k.time_interval >=0  then '6小时以内' when k.time_interval < 0 then '课后取消' end)
        when k.lesson_status='教学质检取消' then '课后取消' end as lesson_cancel_time_dist, --课程取消时间分布,
    

    case when k.cancel_suggestion like '%临时%' then '临时有安排，课程暂时取消' when k.cancel_suggestion like '%时间冲突%' then '时间冲突，课程取消'
         when (k.cancel_suggestion like '%试听前不%' or k.cancel_suggestion like '%失联%') then '试听前失联'  when  k.cancel_suggestion like '%授课老师原因%' then '授课老师原因无法上课' 
         when (k.cancel_suggestion like '%网络%'  or k.cancel_suggestion like '%设备%') then '网络设备问题无法上课' when k.cancel_suggestion like '%主观原因%' then '主观原因不考虑我们机构了'
         when k.cancel_suggestion like '%其他原因%' then '其他原因' when k.cancel_suggestion is null then null else '其他原因' end  as suggestion_type-- 设班单跳票原因类型
from
    (
    select 
            mla.*,
            (case when hflp.student_id is not null then '是' else '否' end) as if_experience,
    --      (case when (select count(*) from lesson_plan where student_id = lp.student_id and lesson_type =3 
    -- and status in (3,5) and solve_status <> 6)>0 then '是' else '否' end) '是否上过体验课',


            case when locate('艺术',concat(coalesce(s.present_school,0),coalesce(otc.content,0),coalesce(lpo.learning_target,0),
                coalesce(lpo.self_evaluation,0),coalesce(lpo.teacher_requirements,0),coalesce(lpo.target_schoolOrmajor,0)))>0 then '艺考'
                             when locate('艺考',concat(coalesce(s.present_school,0),coalesce(otc.content,0),coalesce(lpo.learning_target,0),
                coalesce(lpo.self_evaluation,0),coalesce(lpo.teacher_requirements,0),coalesce(lpo.target_schoolOrmajor,0)))>0 then '艺考'
                             when locate('音乐',concat(coalesce(s.present_school,0),coalesce(otc.content,0),coalesce(lpo.learning_target,0),
                coalesce(lpo.self_evaluation,0),coalesce(lpo.teacher_requirements,0),coalesce(lpo.target_schoolOrmajor,0)))>0 then '音乐'
                             when locate('艺体',concat(coalesce(s.present_school,0),coalesce(otc.content,0),coalesce(lpo.learning_target,0),
                coalesce(lpo.self_evaluation,0),coalesce(lpo.teacher_requirements,0),coalesce(lpo.target_schoolOrmajor,0)))>0 then '体育'
                             when locate('体育',concat(coalesce(s.present_school,0),coalesce(otc.content,0),coalesce(lpo.learning_target,0),
                coalesce(lpo.self_evaluation,0),coalesce(lpo.teacher_requirements,0),coalesce(lpo.target_schoolOrmajor,0)))>0 then '体育'
                             when locate('美术',concat(coalesce(s.present_school,0),coalesce(otc.content,0),coalesce(lpo.learning_target,0),
                coalesce(lpo.self_evaluation,0),coalesce(lpo.teacher_requirements,0),coalesce(lpo.target_schoolOrmajor,0)))>0 then '美术'
                             else '正常课程'  end as is_normal,

            if(hour(mla.adjust_start_time)<9,9,if(hour(mla.adjust_start_time)>=22,22,hour(mla.adjust_start_time))) - if(hour(mla.cancel_time)<9,9,if(hour(mla.cancel_time)>=22,22,
                        hour(mla.cancel_time)))+minute(mla.adjust_start_time)/60 
                            - minute(mla.cancel_time)/60 + datediff(mla.adjust_start_time,mla.cancel_time)*13 as time_interval,--'间隔',
            

            case when opt_user=972038 then 'AI直排'
            when opt_user not in  (381322, 32547,972038) then '教务手动'  
            when opt_user=381322 and locate(hfvui.name,lpo.teacher_requirements) <> 0 then '指定老师'
            when opt_user=381322 and hflpo.order_id is not null then 'APP派单' 
            when opt_user is null then '未排课'  
            else '系统未知'  end as schedule_type, --排课方式





            -- case when opt_user=972038 then 'AI直排'
             --    when opt_user not in  (381322, 32547,972038) then '教务手动'  
             --    when opt_user=381322 
             --    and locate((select name from view_user_info 
             --    where user_id = lp.teacher_id),lpo.teacher_requirements) <> 0 then '指定老师'
             --    when opt_user=381322 and 
             --    ( 
             --    select 
             --       count(*)
             --    from lesson_plan_order  lpo2
             --    left join lesson_plan_schedule lps on lpo2.order_id = lps.order_id
             --    left join lesson_plan_schedule_detail lpsd on lpsd.schedule_id =  lps.id
             --    left join lesson_plan_schedule_detail_2018 lpsdd on lpsdd.schedule_id =  lps.id
             --    where 
             --    (lpo2.inform_status = 5 or lpsd.teacher_status = 5 or lpsdd.teacher_status = 5)
             --    and  lpo2.order_id = lpo.order_id ) > 0 then 'APP派单' 
             --    when opt_user is null then '未排课'  
             --    else '系统未知'  end 排课方式,#排课方式
                

            case when locate('备注',ooh.suggestion) > 0 then SUBSTRING(ooh.suggestion,1,locate('备注',ooh.suggestion)-2) else ooh.suggestion end as cancel_suggestion --设班单跳票原因
                -- (select 
          --         case when locate('备注',ooh.suggestion) > 0 then SUBSTRING(ooh.suggestion,1,locate('备注',ooh.suggestion)-2) else ooh.suggestion end  
          --    from 
          --         order_operate_history ooh where order_id = lpo.order_id and operate_type in (9,10) order by operate_time desc limit 1) 设班单跳票原因

    from hf_mobdb.market_lesson_audio_2  mla
    left join dw_hf_mobdb.dw_lesson_relation lr on lr.plan_id = mla.lesson_plan_id
    left join dw_hf_mobdb.dw_lesson_plan_order lpo on lpo.order_id = lr.order_id
    left join dw_hf_mobdb.dw_order_trial_class otc ON lpo.order_id=otc.order_id
    left join dw_hf_mobdb.dw_lesson_plan lp on lp.lesson_plan_id = mla.lesson_plan_id
    left join dw_hf_mobdb.dw_view_student s on s.student_id = lp.student_id
    left join dw_hf_mobdb.dw_tms_lesson_plan_history tlph on tlph.lesson_plan_id = mla.lesson_plan_id and tlph.opt_type in (1,4)

    left join  dw_hf_mobdb.dw_lesson_plan hflp on hflp.student_id = s.student_id and hflp.lesson_type =3 and hflp.status in (3,5) and hflp.solve_status <> 6


    left join(
        select user_id,name 
        from dw_hf_mobdb.dw_view_user_info vui
        )hfvui on hfvui.user_id = lp.teacher_id
    left join(
        select lpo2.order_id
        from dw_hf_mobdb.dw_lesson_plan_order  lpo2
        left join dw_hf_mobdb.dw_lesson_plan_schedule lps on lpo2.order_id = lps.order_id
        left join dw_hf_mobdb.dw_lesson_plan_schedule_detail lpsd on lpsd.schedule_id =  lps.id
        left join dw_hf_mobdb.dw_lesson_plan_schedule_detail_2018 lpsdd on lpsdd.schedule_id =  lps.id
        where (lpo2.inform_status = 5 or lpsd.teacher_status = 5 or lpsdd.teacher_status = 5) 
        )hflpo on hflpo.order_id = lpo.order_id
   
    left join (
        select order_id,operate_time,suggestion,row_number() over(partition by order_id order by operate_time desc)as rank
        from dw_hf_mobdb.dw_order_operate_history ooh 
        where ooh.operate_type in (9,10) 
        )ooh on ooh.order_id = lpo.order_id -- 设班单跳票原因 

    where mla.adjust_start_time < '2019-01-01' 
    and mla.adjust_start_time < current_date
    and ooh.rank=1
    )k 
;


 --数据集       3.4.3、2019试听数据_每月去重

drop table if exists hf_mobdb.lesson_audio_mon_duplicate_2019;
create table hf_mobdb.lesson_audio_mon_duplicate_2019 as 
select k.*,   
   case when k.lesson_status='取消' then 
        (case when  k.time_interval is null then null when k.time_interval >18 then '18小时以上' when k.time_interval >12 and k.time_interval <= 18 then '12至18小时以内'
        when k.time_interval >=6 and k.time_interval <= 18 then '6至18小时以内' when k.time_interval  <= 6 and k.time_interval >=0  then '6小时以内' when k.time_interval < 0 then '课后取消' end)
        when k.lesson_status='教学质检取消' then '课后取消' end as lesson_cancel_time_dist, --课程取消时间分布,
    

    case when k.cancel_suggestion like '%临时%' then '临时有安排，课程暂时取消' when k.cancel_suggestion like '%时间冲突%' then '时间冲突，课程取消'
         when (k.cancel_suggestion like '%试听前不%' or k.cancel_suggestion like '%失联%') then '试听前失联'  when  k.cancel_suggestion like '%授课老师原因%' then '授课老师原因无法上课' 
         when (k.cancel_suggestion like '%网络%'  or k.cancel_suggestion like '%设备%') then '网络设备问题无法上课' when k.cancel_suggestion like '%主观原因%' then '主观原因不考虑我们机构了'
         when k.cancel_suggestion like '%其他原因%' then '其他原因' when k.cancel_suggestion is null then null else '其他原因' end  as suggestion_type-- 设班单跳票原因类型
from
    (
    select 
            mla.*,
            (case when hflp.student_id is not null then '是' else '否' end) as if_experience,

            case when locate('艺术',concat(coalesce(s.present_school,0),coalesce(otc.content,0),coalesce(lpo.learning_target,0),
                coalesce(lpo.self_evaluation,0),coalesce(lpo.teacher_requirements,0),coalesce(lpo.target_schoolOrmajor,0)))>0 then '艺考'
                             when locate('艺考',concat(coalesce(s.present_school,0),coalesce(otc.content,0),coalesce(lpo.learning_target,0),
                coalesce(lpo.self_evaluation,0),coalesce(lpo.teacher_requirements,0),coalesce(lpo.target_schoolOrmajor,0)))>0 then '艺考'
                             when locate('音乐',concat(coalesce(s.present_school,0),coalesce(otc.content,0),coalesce(lpo.learning_target,0),
                coalesce(lpo.self_evaluation,0),coalesce(lpo.teacher_requirements,0),coalesce(lpo.target_schoolOrmajor,0)))>0 then '音乐'
                             when locate('艺体',concat(coalesce(s.present_school,0),coalesce(otc.content,0),coalesce(lpo.learning_target,0),
                coalesce(lpo.self_evaluation,0),coalesce(lpo.teacher_requirements,0),coalesce(lpo.target_schoolOrmajor,0)))>0 then '体育'
                             when locate('体育',concat(coalesce(s.present_school,0),coalesce(otc.content,0),coalesce(lpo.learning_target,0),
                coalesce(lpo.self_evaluation,0),coalesce(lpo.teacher_requirements,0),coalesce(lpo.target_schoolOrmajor,0)))>0 then '体育'
                             when locate('美术',concat(coalesce(s.present_school,0),coalesce(otc.content,0),coalesce(lpo.learning_target,0),
                coalesce(lpo.self_evaluation,0),coalesce(lpo.teacher_requirements,0),coalesce(lpo.target_schoolOrmajor,0)))>0 then '美术'
                             else '正常课程'  end as is_normal,

            if(hour(mla.adjust_start_time)<9,9,if(hour(mla.adjust_start_time)>=22,22,hour(mla.adjust_start_time))) - if(hour(mla.cancel_time)<9,9,if(hour(mla.cancel_time)>=22,22,
                        hour(mla.cancel_time)))+minute(mla.adjust_start_time)/60 
                            - minute(mla.cancel_time)/60 + datediff(mla.adjust_start_time,mla.cancel_time)*13 as time_interval,--'间隔',           

            case when opt_user=972038 then 'AI直排'
            when opt_user not in  (381322, 32547,972038) then '教务手动'  
            when opt_user=381322 and locate(hfvui.name,lpo.teacher_requirements) <> 0 then '指定老师'
            when opt_user=381322 and hflpo.order_id is not null then 'APP派单' 
            when opt_user is null then '未排课'  
            else '系统未知'  end as schedule_type, --排课方式
            case when locate('备注',ooh.suggestion) > 0 then SUBSTRING(ooh.suggestion,1,locate('备注',ooh.suggestion)-2) else ooh.suggestion end as cancel_suggestion, --设班单跳票原因

             
        hfvs.track_userid,
        hfvs.track_user_name,
        hfvs.track_job_number
    from hf_mobdb.market_lesson_audio_2  mla
    left join dw_hf_mobdb.dw_lesson_relation lr on lr.plan_id = mla.lesson_plan_id
    left join dw_hf_mobdb.dw_lesson_plan_order lpo on lpo.order_id = lr.order_id
    left join dw_hf_mobdb.dw_order_trial_class otc ON lpo.order_id=otc.order_id
    left join dw_hf_mobdb.dw_lesson_plan lp on lp.lesson_plan_id = mla.lesson_plan_id
    left join dw_hf_mobdb.dw_view_student s on s.student_id = lp.student_id
    left join dw_hf_mobdb.dw_tms_lesson_plan_history tlph on tlph.lesson_plan_id = mla.lesson_plan_id and tlph.opt_type in (1,4)

    left join  dw_hf_mobdb.dw_lesson_plan hflp on hflp.student_id = s.student_id and hflp.lesson_type =3 and hflp.status in (3,5) and hflp.solve_status <> 6

    left join(
        select user_id,name 
        from dw_hf_mobdb.dw_view_user_info vui
        )hfvui on hfvui.user_id = lp.teacher_id
    left join(
        select lpo2.order_id
        from dw_hf_mobdb.dw_lesson_plan_order  lpo2
        left join dw_hf_mobdb.dw_lesson_plan_schedule lps on lpo2.order_id = lps.order_id
        left join dw_hf_mobdb.dw_lesson_plan_schedule_detail lpsd on lpsd.schedule_id =  lps.id
        left join dw_hf_mobdb.dw_lesson_plan_schedule_detail_2018 lpsdd on lpsdd.schedule_id =  lps.id
        where (lpo2.inform_status = 5 or lpsd.teacher_status = 5 or lpsdd.teacher_status = 5) 
        )hflpo on hflpo.order_id = lpo.order_id
   
    left join (
        select order_id,operate_time,suggestion,row_number() over(partition by order_id order by operate_time desc)as rank
        from dw_hf_mobdb.dw_order_operate_history ooh 
        where ooh.operate_type in (9,10) 
        )ooh on ooh.order_id = lpo.order_id -- 设班单跳票原因 
    
     left join(
        select tpel.intention_id,
            tpel.track_userid ,
            ui.name as track_user_name,
            ui.job_number as track_job_number,
            tpel.into_pool_date as into_pool_date,
            row_number() over (partition by tpel.track_userid order by tpel.into_pool_date desc )as rank
        from dw_hf_mobdb.dw_tms_pool_exchange_log tpel 
        left join  dw_hf_mobdb.dw_jw_abnormal_user jau on tpel.track_userid=jau.user_id
        left join  dw_hf_mobdb.dw_view_user_info ui on tpel.track_userid=ui.user_id
        where jau.user_id is null 
        )hfvs on hfvs.intention_id = s.student_intention_id
    where mla.adjust_start_time>= '2019-01-01'
    and mla.adjust_start_time < current_date
    and ooh.rank=1
    and hfvs.into_pool_date<mla.adjust_start_time
    )k 
;



    --数据集 3.4、试听-拼接
drop table if exists hf_mobdb.lesson_audio_contract;
create table hf_mobdb.lesson_audio_contract as
select 
    a.lesson_plan_id
    ,a.order_id
    ,a.lesson_status
    ,a.adjust_start_time
    ,a.adjust_end_time
    ,a.start_month
    ,a.end_month
    ,a.subject_name
    ,a.exam_year
    ,a.city
    ,a.create_time
    ,a.create_date
    ,a.student_no
    ,a.student_name
    ,a.teacher_name
    ,a.ADID
    ,a.origin
    ,a.adid_account
    ,a.plan
    ,a.key_word
    ,a.coil
    ,a.submit_kf
    ,a.student_province
    ,a.is_expand
    ,a.is_first
    ,a.follow_sales
    ,a.timetable_date
    ,a.s_phone7
    ,a.apply_user_id
    ,a.apply_user_name
    ,a.apply_user_department
    ,a.update_time as last_update_time
    ,a.cancel_time
    ,a.device
    ,a.apply_time
    ,a.schedule_type
    ,a.if_experience
    ,a.time_interval
    ,cast(a.track_userid as string)track_userid
    ,a.track_user_name
    ,a.track_job_number
    ,a.lesson_cancel_time_dist
    ,'' cancel_suggestion
from hf_mobdb.market_lesson_audio_bidata_2019 a 
where to_date(a.adjust_start_time) >= '2019-10-01'
union 
select 
    a.lesson_plan_id
    ,a.order_id
    ,a.lesson_status
    ,a.adjust_start_time
    ,a.adjust_end_time
    ,a.start_month
    ,a.end_month
    ,a.subject_name
    ,a.exam_year
    ,a.city
    ,a.create_time
    ,a.create_date
    ,a.student_no
    ,a.student_name
    ,a.teacher_name
    ,a.ADID
    ,a.origin
    ,a.adid_account
    ,a.plan
    ,a.key_word
    ,a.coil
    ,a.submit_kf
    ,a.student_province
    ,a.is_expand
    ,a.is_first
    ,a.follow_sales
    ,a.timetable_date
    ,a.s_phone7
    ,a.apply_user_id
    ,a.apply_user_name
    ,a.apply_user_department
    ,a.update_time last_update_time
    ,a.cancel_time
    ,a.device
    ,a.apply_time
    ,a.schedule_type
    ,a.if_experience
    ,a.time_interval --
    ,cast(a.track_userid as string)track_userid
    ,a.track_user_name
    ,a.track_job_number
    ,a.lesson_cancel_time_dist
    ,a.cancel_suggestion
from hf_mobdb.lesson_audio_mon_duplicate_2019 a 
where to_date(a.adjust_start_time) <= '2019-10-01'
union 
select 
    a.lesson_plan_id
    ,a.order_id
    ,a.lesson_status
    ,a.adjust_start_time
    ,a.adjust_end_time
    ,a.start_month
    ,a.end_month
    ,a.subject_name
    ,a.exam_year
    ,a.city
    ,a.create_time
    ,a.create_date
    ,a.student_no
    ,a.student_name
    ,a.teacher_name
    ,a.ADID
    ,a.origin
    ,a.adid_account
    ,a.plan
    ,a.key_word
    ,a.coil
    ,a.submit_kf
    ,a.student_province
    ,a.is_expand
    ,a.is_first
    ,a.follow_sales
    ,a.timetable_date
    ,a.s_phone7
    ,a.apply_user_id
    ,a.apply_user_name
    ,a.apply_user_department
    ,a.update_time last_update_time
    ,a.cancel_time
    ,a.device
    ,a.apply_time
    ,a.schedule_type
    ,a.if_experience
    ,a.time_interval
    ,'' track_userid
    ,'' track_user_name
    ,'' track_job_number
    ,a.lesson_cancel_time_dist
    ,a.cancel_suggestion
from hf_mobdb.lesson_audio_mon_duplicate_2018 a
;  


drop table if exists hf_mobdb.submit_2018_now;
create table hf_mobdb.submit_2018_now as
select t1.*
    ,ca.channel,ca.channel_gather,ca.type,ca.owntype,ca.owner
    ,if(t1.stage='试听阶段',1,0)as shiting_stage
    ,if(t1.stage='成单',1,0)as chengdan_stage
    ,if(t1.stage='待成单阶段',1,0)as daichengdan_stage
    ,if(t1.stage_oc="未接听",1,0)as weijieting
    ,if(t1.stage='无效',1,0)as wuxiao
    ,ssd.this_student_longest_time
    ,if(t1.is_valid='是',1,0)as if_valid
    ,case when t1.name ='新名单空号池'  or t1.name ='新名单停机池' then 1 else 0 end as tingjikonghao
    ,case when intention_big_type = '香港高校' then '升学'
        when intention_big_type = '留学申请' then '升学'
        when intention_big_type = '春季高考' then '升学'
        when intention_big_type = '志愿填报' then '升学'
        when intention_big_type = '自主招生' then '升学'
        when intention_big_type = '三位一体' then '升学'
        when intention_big_type = '上纽大' then '升学'
        when intention_big_type = '1对6学科' then '学科'
        when intention_big_type = '学科辅导' then '学科'
        when intention_big_type = '一元首课' then '学科'
        when intention_big_type = '5小时活动' then '学科'
        when intention_big_type = '公益服务' then '学科'
        when intention_big_type = '录播课' then '学科'
        when intention_big_type = '半价活动' then '学科'
        when intention_big_type = '昆山杜克' then '升学' else '学科'  end  as kechengleixing
from(

    select 
        wss.create_time,
        wss.submit_time,
        wss.student_no,
        wss.adid_account,
        wss.ADID,
        wss.key_word,
        wss.plan,
        wss.know_origin,
        -- dd.value coil_in,
        wss.exam_year,
        wss.city,
        wss.province,
        wss.intention_big_type,
        wss.earliest_intention,
        wss.stage,
        wss.stage_oc,
        wss.name,
        wss.communication_user_id,-- 第一个沟通销售id,
        wss.communication_user_name,-- 第一个沟通销售姓名,
        -- wss.job_number,-- 第一个沟通销售工号,
        wss.communication_department_name,-- 第一个沟通销售所在组别,
        -- wss.communication_end_time, -- 第一次沟通时间
        '' as is_valid,
        substr(wss.communication_end_time,1,10)as communication_time --沟通时间
    from dw_hf_mobdb.dw_will_student_submit_20180106 wss
    left join dw_hf_mobdb.dw_view_student s on s.student_no = wss.student_no
    left join dw_hf_mobdb.dw_ddic dd on s.coil_in=dd.code and dd.type='TP023'
union
    select ssc.create_time
        ,ssc.submit_time
        ,ssc.student_no
        ,ssc.adid_account
        ,ssc.adid
        ,ssc.key_word
        ,ssc.plan
        ,ssc.know_origin
        -- ,ssc.coil_in
        ,ssc.exam_year
        ,ssc.city
        ,ssc.province
        ,ssc.intention_big_type
        ,ssc.earliest_intention
        ,ssc.stage
        ,ssc.stage_oc
        ,ssc.name
        ,ssc.communication_user_id
        ,ssc.communication_user_name
        -- ,ssc.communication_end_time
        ,ssc.communication_department_name
        ,ssc.is_valid
        ,substr(ssc.communication_end_time,1,10)as communication_time
    from hf_mobdb.student_submit_chenjia ssc
union
    select sscdh.create_time
        ,sscdh.submit_time
        ,sscdh.student_no
        ,sscdh.adid_account
        ,sscdh.adid
        ,sscdh.key_word
        ,sscdh.plan
        ,sscdh.know_origin
        -- ,dd.value as coil_in
        ,sscdh.exam_year
        ,sscdh.city
        ,sscdh.province
        ,sscdh.intention_big_type
        ,sscdh.earliest_intention
        ,sscdh.stage
        ,sscdh.stage_oc
        ,sscdh.name
        ,sscdh.communication_user_id
        ,sscdh.communication_user_name
        -- ,sscdh.communication_end_time
        ,sscdh.communication_department_name
        ,sscdh.is_valid
        ,substr(sscdh.communication_end_time,1,10)as communication_time
    from dw_hf_mobdb.dw_student_submit_chenjia_2018_down_half sscdh
    left join  dw_hf_mobdb.dw_view_student s on s.student_no = sscdh.student_no
    left join  dw_hf_mobdb.dw_ddic dd  on s.coil_in=dd.code and dd.type='TP023'
    )t1
left join dw_hf_mobdb.dw_zjj_channel_account ca on t1.adid_account=ca.account
left join (
    select distinct student_no,  
        this_student_longest_time -- as'该学生最长通话时长',
    from hf_mobdb.shaobin_student_dimension
    )ssd on t1.student_no=ssd.student_no
;

drop table if exists hf_mobdb.register_2018_now;
create table hf_mobdb.register_2018_now as
select t1.create_time,t1.student_no,t1.student_name,t1.adid_account,t1.ADID,t1.key_word
    ,t1.coil,t1.plan,t1.know_origin,t1.exam_year,t1.city,t1.province,t1.intention_big_type
    ,t1.earliest_intention,t1.name,t1.stage,t1.stage_oc,t1.last_chage_adid_time
    ,ca.channel,ca.channel_gather,ca.type,ca.owntype,ca.owner
    ,COALESCE(t1.last_chage_adid_time,t1.create_time)as create_time1
    ,case when last_chage_adid_time>create_time then '重复' else '非重复' end as shifouchognfu

from(
    

    select xr.create_time,xr.student_no,xr.student_name,xr.adid_account,xr.ADID,xr.key_word
        ,xr.coil,xr.plan,xr.know_origin,xr.exam_year,xr.city,xr.province,xr.intention_big_type
        ,xr.earliest_intention,xr.name,xr.stage,xr.stage_oc,xr.last_chage_adid_time,xr.which_week
        ,xr.llast_week
    from hf_mobdb.xiaozhen_2019_register xr
union

    select 
        s.create_time,
        s.student_no,
        s.name student_name,
        taa.account adid_account,
        adid_no ADID,
        adid.originality key_word,
        dd1.value coil,
        tap.plan_name plan,
        dd2.value know_origin,

        s.exam_year,
        (case when mpc.regLoc is null then '其他' else mpc.regLoc end) city,
        (case when ar.area_name is null then '其他' else mpc.regLoc end) province,

        dd3.value intention_big_type,
        dd4.value earliest_intention,
        ui.name,
        (case when (s.earliest_intention=9 and s.sale_stage2=0) or s.sale_stage2 in(1,3) then '新名单跟进阶段'
                    when  s.sale_stage2 in(6,9,10,11,7,8,12) then '试听阶段' when  s.sale_stage2 in(13,14) then '待成单阶段'
                    when  s.sale_stage2 =15 then '成单'  when  s.sale_stage2 =2 then '无效' when  s.sale_stage2 =4 then '未试听'
                    when s.earliest_intention<>9 and  s.sale_stage2 =0 then 'oc新名单' else '其他' end) stage,

        (case when s.earliest_intention<>9 and  s.sale_stage2 =0 then dd5.value else dd6.value end )stage_oc,

        tacl.last_chage_adid_time,
        weekofyear(substr(s.create_time,1,19)) which_week,
        weekofyear(current_date) + 50 llast_week

    from dw_hf_mobdb.dw_view_student s 
    left join dw_hf_mobdb.dw_adid adid on s.adid_Id = adid.adid_Id
    left join dw_hf_mobdb.dw_tms_adid_account taa on adid.account_code = taa.id
    left join dw_hf_mobdb.dw_ddic dd1 on s.coil_in=dd1.code and dd1.type='TP023' 
    left join dw_hf_mobdb.dw_tms_adid_plan tap on adid.plan_code = tap.id
    left join dw_hf_mobdb.dw_ddic dd2 on s.know_origin=dd2.code and dd2.type='TP016'
    left join dw_hf_mobdb.dw_ddic dd3 on dd3.code = s.intention_big_type and dd3.type = 'TP046' 
    left join dw_hf_mobdb.dw_ddic dd4 on s.earliest_intention=dd4.code and dd4.type='TP065' 

    left join(
            select student_intention_id,max(sale_stage2) sale_stage2 
            from dw_hf_mobdb.dw_tms_sale_stage_history 
            group by student_intention_id
            ) x on x.student_intention_id=s.student_intention_id
    left join dw_hf_mobdb.dw_ddic dd5 on dd5.code=x.sale_stage2 and dd5.type='ST1111' 
    left join dw_hf_mobdb.dw_ddic dd6 on dd6.code=s.sale_stage2 and dd6.type='ST1111'
    left join(
        select student_intention_id,max(concat(opt_date,' ',opt_time))as last_chage_adid_time
        from dw_hf_mobdb.dw_tms_adid_change_log
        where opt_user='系统'
        and adid_Id <> adid_id_old
        group by student_intention_id
        )tacl on tacl.student_intention_id = s.student_intention_id

    left join dw_hf_mobdb.dw_map_phone_city_copy mpc on substr(s.phone,1,7)=mpc.phone7
    left join dw_hf_mobdb.dw_area ar on (mpc.provincenum*10000)=ar.id
    left join dw_hf_mobdb.dw_view_user_info ui on s.track_userid=ui.user_id
    where s.create_time>='2018-01-01'  
    and s.create_time<'2019-01-01' 
    and s.account_type =1
    )t1
left join dw_hf_mobdb.dw_zjj_channel_account ca on t1.adid_account=ca.account
;



--数据集 3、销售环节率值_ID拼接 etl

drop table if exists hf_mobdb.sales_value_id_contract_1;
create table  hf_mobdb.sales_value_id_contract_1 as
select t1.sign_time 
    ,t1.id
    ,owntype
    ,type
    ,department_name
    ,suoshufenlei 
    ,tjs
    ,zjstjs
    ,tjs_youxiao
    ,zjstjs_youxiao
    ,'' is_expand
    ,'' is_first
    ,'' sts
    ,'' tpsj
    ,'' ycs
    ,'' tpsx
    ,'' zjstpsj
    ,'' zjstpsx
    ,'' yyst
    ,'' zjssts
    ,'' zjswcsts
    ,'' shangtiyanke
    ,'' tpshangtiyanke
    ,'' wcstshangtiyanke
    ,'' zjsshangtiyanke
    ,'' qys
    ,'' qyje
    ,'' zjsqys
    ,'' zjsqyje
    ,'' if_new_sign_finance
from(

    select a.communication_time as sign_time
        ,cast(a.communication_user_id as string) as id
        ,a.owntype
        ,a.type
        ,a.communication_department_name as department_name
        ,a.suoshufenlei
        ,cast(count(distinct a.student_no) as string)tjs
        ,cast(sum(a.zhuanjieshao)as string) zjstjs
        ,cast(sum(case when a.communication_time < '2018-11-01' then 1 else if_valid end )as string)tjs_youxiao
        ,cast(sum(case when a.communication_time < '2018-11-01' and suoshufenlei='转介绍' then 1 
            when a.communication_time >= '2018-11-01' and a.suoshufenlei='转介绍' then if_valid else 0 end)as string)zjstjs_youxiao
    from(
        select t1.*
            ,case when  channel ='家长推荐' or  channel='老师推荐' or channel='老学员推荐' or channel='员工推荐'or channel='学员转介绍' or channel='转介绍' 
                or coil='转介绍' or coil='学员转介绍' or coil='活动转介绍' then '转介绍' else '非转介绍' end as suoshufenlei
            ,case when  channel ='家长推荐' or  channel='老师推荐' or channel='老学员推荐' or channel='员工推荐'or channel='学员转介绍' or channel='转介绍' 
                or coil='转介绍' or coil='学员转介绍' or coil='活动转介绍' then 1 else 0 end as zhuanjieshao
            ,substr(submit_time,1,10)as submit_time1
        from(       
            select sn.*
                ,rn.coil
                ,case when sn.intention_big_type = '香港高校' then '升学'
                    when sn.intention_big_type = '留学申请' then '升学'
                    when sn.intention_big_type = '春季高考' then '升学'
                    when sn.intention_big_type = '志愿填报' then '升学'
                    when sn.intention_big_type = '自主招生' then '升学'
                    when sn.intention_big_type = '三位一体' then '升学'
                    when sn.intention_big_type = '上纽大' then '升学'
                    when sn.intention_big_type = '1对6学科' then '学科'
                    when sn.intention_big_type = '学科辅导' then '学科'
                    when sn.intention_big_type = '一元首课' then '学科'
                    when sn.intention_big_type = '5小时活动' then '学科'
                    when sn.intention_big_type = '公益服务' then '学科'
                    when sn.intention_big_type = '录播课' then '学科'
                    when sn.intention_big_type = '半价活动' then '学科'
                    when sn.intention_big_type = '昆山杜克' then '升学'  else '学科' end  as xuekeleixing
            from hf_mobdb.submit_2018_now sn
            left join hf_mobdb.register_2018_now rn on sn.student_no=rn.student_no
            )t1
        where t1.xuekeleixing = '学科'  
        and t1.communication_department_name not  like '%测试%'
        and t1.communication_user_name not like '%测试%'
        )a
    group by a.communication_time,a.communication_user_id,a.owntype,a.type,a.communication_department_name,a.suoshufenlei
    )t1

union 
select t2.sign_time 
    ,t2.id
    ,owntype
    ,type
    ,department_name
    ,suoshufenlei 
    ,'' tjs
    ,'' zjstjs
    ,'' tjs_youxiao
    ,'' zjstjs_youxiao
    ,is_expand
    ,is_first
    ,sts
    ,tpsj
    ,ycs
    ,tpsx
    ,zjstpsj
    ,zjstpsx
    ,yyst
    ,zjssts
    ,zjswcsts
    ,shangtiyanke
    ,tpshangtiyanke
    ,wcstshangtiyanke
    ,zjsshangtiyanke
    ,'' qys
    ,'' qyje
    ,'' zjsqys
    ,'' zjsqyje
    ,'' if_new_sign_finance
from(
    select shiting_date as sign_time 
        ,track_userid as id
        ,owntype
        ,type
        ,department_name
        ,suoshufenlei 
        ,is_expand
        ,is_first
        ,cast(sum(wanchengshiting)as string) sts
        ,cast(sum(tiaopiaoshuj)as string) tpsj
        ,cast(sum(yichangshuj)as string) ycs
        ,cast(sum(tiaopiaoshux)as string) tpsx
        ,cast(sum(zjstiaopiaoshuj)as string) zjstpsj
        ,cast(sum(zjstiaopiaoshux)as string) zjstpsx
        ,cast(count(student_no)as string) yyst
        ,cast(sum(zhuanjieshao)as string) zjssts
              
        ,cast(sum(zjswanchengshiting) as string) zjswcsts
        ,cast(sum(tiyanke) as string)shangtiyanke
        ,cast(sum(tptiyanke)as string) tpshangtiyanke
        ,cast(sum(wcsttiyanke) as string)wcstshangtiyanke
        ,cast(sum(zjstiyanke) as string)zjsshangtiyanke
    from(
        select a.*
            ,case when  lesson_status in ('系统','非系统') and shiting_date<dangtian_date then 1 else 0 end as wanchengshiting
            ,case when  lesson_status in ('系统','非系统') and suoshufenlei='转介绍' and shiting_date<dangtian_date 
                then 1 else 0 end as zjswanchengshiting
            ,case when lesson_status in ('取消') and shiting_date<dangtian_date then 1 else 0 end as tiaopiaoshuj
            ,case when lesson_status in ('教学质检取消') and shiting_date<dangtian_date then 1 else 0 end as yichangshuj
            ,case when shifoutiaopiao='跳票' and shiting_date<dangtian_date then 1 else 0 end as tiaopiaoshux
            ,case when lesson_status in ('取消') and suoshufenlei='转介绍' and shiting_date<dangtian_date then 1 else 0 end as zjstiaopiaoshuj
            ,case when shifoutiaopiao='跳票' and suoshufenlei='转介绍' and shiting_date<dangtian_date then 1 else 0 end as zjstiaopiaoshux
            ,case when if_experience = '是' then 1 else 0 end as tiyanke
            ,case when if_experience = '是'  and lesson_status in ('取消') and shiting_date<dangtian_date then 1 else 0 end as tptiyanke
            ,case when if_experience = '是' and lesson_status in ('系统','非系统') and  shiting_date<dangtian_date then 1 else 0 end as wcsttiyanke
            ,case when if_experience = '是' and suoshufenlei='转介绍' then 1 else 0 end as zjstiyanke
            ,cdme.department_name
            
        from(
            select lac.*
                ,case when  channel ='家长推荐' or  channel='老师推荐' or channel='老学员推荐' or channel='员工推荐'or channel='学员转介绍' or channel='转介绍' 
                        or coil='转介绍' or coil='学员转介绍' or coil='活动转介绍' then '转介绍' else '非转介绍' end as suoshufenlei
                ,case when  channel ='家长推荐' or  channel='老师推荐' or channel='老学员推荐' or channel='员工推荐'or channel='学员转介绍' or channel='转介绍' 
                    or coil='转介绍' or coil='学员转介绍' or coil='活动转介绍' then 1 else 0 end as zhuanjieshao

                ,case when lesson_cancel_time_dist in ('6小时以内','课后取消') then '跳票' else '非跳票' end shifoutiaopiao
                ,substr(adjust_start_time,1,7)as shiting_date --试听日期
                ,current_date as dangtian_date --当天日期
                ,COALESCE(track_userid,apply_user_id)as shitingke_owner --试听课归属人
                ,ca.channel,ca.type,ca.owntype
            from hf_mobdb.lesson_audio_contract lac
            left join dw_hf_mobdb.dw_zjj_channel_account ca on lac.adid_account=ca.account
            )a
        left join dt_mobdb.dt_charlie_dept_month_end cdme on a.shitingke_owner=cdme.user_id and cdme.stats_date = current_date
        where shiting_date<dangtian_date
        )b
    group by shiting_date ,track_userid,owntype
        ,type
        ,department_name
        ,suoshufenlei 
        ,is_expand
        ,is_first
    )t2

union 

select t3.sign_time 
    ,t3.id
    ,owntype
    ,type
    ,department_name
    ,suoshufenlei 
    ,'' tjs
    ,'' zjstjs
    ,'' tjs_youxiao
    ,'' zjstjs_youxiao
    ,'' is_expand
    ,'' is_first
    ,'' sts
    ,'' tpsj
    ,'' ycs
    ,'' tpsx
    ,'' zjstpsj
    ,'' zjstpsx
    ,'' yyst
    ,'' zjssts
    ,'' zjswcsts
    ,'' shangtiyanke
    ,'' tpshangtiyanke
    ,'' wcstshangtiyanke
    ,'' zjsshangtiyanke
    ,qys
    ,qyje
    ,zjsqys
    ,zjsqyje
    ,if_new_sign_finance
from(
    select sign_date as sign_time
        ,submit_user_id as id--销售id
        ,if_new_sign_finance
        ,owntype
        ,type
        ,suoshufenlei
        ,subuser_depart_name as department_name
        ,cast(count(distinct student_no)as string) qys
        ,cast(sum(contract_amount)as string) qyje
        ,cast(sum(zhuanjieshao)as string) zjsqys
        ,cast(sum(zhuanjieshaojine)as string) zjsqyje
    from(
        select cj.*
            ,substr(order_submit_date,1,10)as sign_date --签约日期
            ,IF(formal_period<40,'0-40',IF(formal_period<80,'40-80',IF(formal_period<120,'80-120'
                ,IF(formal_period<160,'120-160',IF(formal_period<200,'160-200',IF(formal_period<240,'200-240','240以上')))))) as keshibao_dist --课时包分布
            ,case when  channel ='家长推荐' or  channel='老师推荐' or channel='老学员推荐' or channel='员工推荐'or channel='学员转介绍' or channel='转介绍' 
                or in_line='转介绍' or in_line='学员转介绍' or in_line='活动转介绍' then '转介绍' else '非转介绍' end as suoshufenlei
            ,case when  channel ='家长推荐' or  channel='老师推荐' or channel='老学员推荐' or channel='员工推荐'or channel='学员转介绍' or channel='转介绍' 
                        or in_line='转介绍' or in_line='学员转介绍' or in_line='活动转介绍' then 1 else 0 end as zhuanjieshao
            ,case when channel ='转介绍' or in_line ='转介绍' then contract_amount else 0 end  zhuanjieshaojine
        from hf_mobdb.contract_joint cj
        )a
    group by  sign_date,submit_user_id,if_new_sign_finance,owntype,type,suoshufenlei,subuser_depart_name
    )t3 
;



drop table if exists hf_mobdb.sales_value_id_contract_2;
create table  hf_mobdb.sales_value_id_contract_2 as

select b.*
    ,case when month(sign_time_f)=month(on_board_date_f) and year(sign_time_f)=year(on_board_date_f) and month(sign_time_f)=month(last_depart_date_f) 
        and year(sign_time_f)=year(last_depart_date_f) then lastdepart_onboard
        when month(sign_time_f)=month(on_board_date_f) and year(sign_time_f)=year(on_board_date_f) and month(sign_time_f)<>month(last_depart_date_f) then lastfay_onboard
        when month(sign_time_f)<>month(on_board_date_f) and month(sign_time_f)=month(last_depart_date_f) and year(sign_time_f)=year(last_depart_date_f) then lastdepart_firstday 
        when month(sign_time_f)<>month(on_board_date_f) and month(sign_time_f)<>month(last_depart_date_f) then dangyue_days else dangyue_days end as zaizhitianshu 

    ,case when month(sign_time_f)=month(on_board_date_f) and year(sign_time_f)=year(on_board_date_f) and month(sign_time_f)=month(last_depart_date_f) 
        and year(sign_time_f)=year(last_depart_date_f) then lastdepart_onboard/b.dangyue_days
        when month(sign_time_f)=month(on_board_date_f) and year(sign_time_f)=year(on_board_date_f) and month(sign_time_f)<>month(last_depart_date_f) then lastfay_onboard/b.dangyue_days
        when month(sign_time_f)<>month(on_board_date_f) and month(sign_time_f)=month(last_depart_date_f) and year(sign_time_f)=year(last_depart_date_f) then lastdepart_firstday/b.dangyue_days 
        when month(sign_time_f)<>month(on_board_date_f) and month(sign_time_f)<>month(last_depart_date_f) then dangyue_days/b.dangyue_days else dangyue_days/b.dangyue_days end as jiaquanrenxiao
from(
    select a.sign_time,a.id,a.owntype,a.type,a.department_name,a.suoshufenlei,a.tjs,a.zjstjs,a.tjs_youxiao,a.zjstjs_youxiao,a.is_expand
        ,a.is_first,a.sts,a.tpsj,a.ycs,a.tpsx,a.zjstpsj,a.zjstpsx,a.yyst,a.zjssts,a.zjswcsts,a.shangtiyanke,a.tpshangtiyanke,a.wcstshangtiyanke
        ,a.zjsshangtiyanke,a.qys,a.qyje,a.zjsqys,a.zjsqyje,a.if_new_sign_finance
        ,a.name,a.job_number,a.on_board_date_change,a.last_depart_date
        ,a.on_board_date_interval,a.on_board_date_f,a.sign_time_f,a.dangyue_days,a.dangyue_lastday,a.last_depart_date_f
        ,IF(a.on_board_date_interval>=60 or a.on_board_date_change='两月以前' ,'老员工','新员工')as if_new_job --新老员工
        ,DATE_FORMAT(sign_time,'yyyy/MM') as month--月份
        ,date_format(sign_time,'yyyy-MM-01') as mon_fristday --当月第一天
        ,DATEDIFF(last_depart_date_f,on_board_date_f)as lastdepart_onboard --最后在岗-入职
        ,DATEDIFF(dangyue_lastday,on_board_date_f)as lastfay_onboard --最后一天-入职
        ,DATEDIFF(last_depart_date_f,date_format(sign_time,'yyyy-MM-01'))as lastdepart_firstday --最后在岗-当月第一天
    from(
        select svic.sign_time,id,owntype,type,department_name,suoshufenlei,tjs
            ,svic.zjstjs,svic.tjs_youxiao,svic.zjstjs_youxiao,svic.is_expand,svic.is_first,svic.sts,svic.tpsj,svic.ycs,svic.tpsx,svic.zjstpsj,svic.zjstpsx
            ,svic.yyst,svic.zjssts,svic.zjswcsts,svic.shangtiyanke,svic.tpshangtiyanke,svic.wcstshangtiyanke,svic.zjsshangtiyanke
            ,svic.qys,svic.qyje,svic.zjsqys,svic.zjsqyje,svic.if_new_sign_finance               
            ,hfcs.name,hfcs.job_number,hfcs.on_board_date_change,hfcs.last_depart_date
            ,datediff(svic.sign_time,hfcs.on_board_date_change)as on_board_date_interval --入职时间间隔
            ,to_date(hfcs.on_board_date_change)as on_board_date_f --入职时间-日期格式
            ,to_date(svic.sign_time)as sign_time_f
            ,DAYOFMONTH(LAST_DAY(svic.sign_time))as dangyue_days --当月天数
            ,last_day(svic.sign_time)as dangyue_lastday
            ,to_date(hfcs.last_depart_date)as last_depart_date_f --最后在岗时间-日期格式
        from hf_mobdb.sales_value_id_contract_1 svic
        left join(
            select cs.name,cs.job_number,cs.user_id
                ,case when coalesce(on_board_date,substr(reg_date_repair,1,10)) is null then '两月以前' else coalesce(on_board_date,substr(reg_date_repair,1,10))  end as on_board_date_change--入职时间-改
                ,coalesce(if(length(depart_date)<7,'null',depart_date),last_commu_time)as last_depart_date--最后在岗时间
            from hf_mobdb.crm_stucture cs
            )hfcs on svic.id=hfcs.user_id
        )a
    )b
;



drop table if exists hf_mobdb.sales_value_id_contract_3;
create table  hf_mobdb.sales_value_id_contract_3 as

select s2.sign_time,s2.month,id,if_new_job,if_new_sign_finance,type,owntype,department_name,is_expand
    ,is_first,on_board_date_change,jiaquanrenxiao,last_depart_date_f
    ,sum(tjs)as tjs_sum--提交数
    ,sum(tjs_youxiao) as tjs_youxiao_sum --提交数_有效
    ,sum(zjstjs_youxiao) as zjstjs_youxiao --转介绍提交数_有效
    ,sum(yyst) as yyst_sum --预约试听数
    ,sum(shangtiyanke) as shangtiyanke_sum --上体验课人数
    ,sum(tpsj)as tpsj_sum  --跳票数
    ,sum(tpsx) as tpsx_sum --跳票数-新
    ,sum(ycs)as ycs_sum --异常数
    ,sum(sts)as sts_sum --试听数
    ,sum(qys)  qys_sum --签约数
    ,sum(qyje)  qyje_sum --签约金额
    ,sum(zjstjs) zjstjs_sum --装介绍沟通数
    ,sum(zjssts)  zjssts_sum --转介绍预约试听数
    ,sum(zjswcsts) zjswcsts_sum --转介绍完成试听数
    ,sum(zjsqys) zjsqys_sum --转介绍签约数
    ,sum(zjsqyje) zjsqyje_sum --转介绍签约金额
    ,sum(zjstpsj) zjstpsj_sum --转介绍跳票数
    ,sum(zjsshangtiyanke) zjsshangtiyanke_sum --转介绍上体验课数
    ,sum(zjstpsx) zjstpsx_sum  --转介绍跳票数-新
    ,sum(tpshangtiyanke)  tpshangtiyanke_sum --跳票上体验课数
    ,sum(wcstshangtiyanke) wcstshangtiyanke_sum --完成试听上体验课数
from hf_mobdb.sales_value_id_contract_2 s2
group by s2.sign_time,s2.month,id,if_new_job,if_new_sign_finance,type,owntype,department_name,is_expand
    ,is_first,on_board_date_change,jiaquanrenxiao,last_depart_date_f
;

--数据集 销售通时通次红黑榜排名_含业绩试听 tmp1

drop table if exists hf_mobdb.sales_tongshi_tongci_rank_1;
create table hf_mobdb.sales_tongshi_tongci_rank_1 as 

    select jshc.stats_date  --日期
        ,jshc.user_id --销售id
        ,jshc.name    --销售姓名
        ,jshc.job_number  --工号
        ,cdme.quarters --岗位
        ,cdme.department_name as xiaoshou_type      
        ,cdme.branch  
        ,cdme.center  
        ,cdme.region  
        ,cdme.department 
        ,attend_time  --打卡时间, 
        ,call_time_total/3600 as call_time_total_hour  --天润通时
        ,phone_seconds/3600 as phone_seconds_hour  --   工作手机通时,
        ,class_period/60 as class_period_hour --体验课时长

        ,if(call_con_count_total is null,0,call_con_count_total) as call_con_count_total --   天润接通次数,
        ,if(call_count_total is null,0,call_count_total)as call_count_total   --天润拨打次数,
        ,cast(if(phone_call_cnt is null,0,phone_call_cnt)as string) phone_call_cnt  -- 工作手机通次,
        ,phone_bridge_cnt  --工作手机接通通次,

        ,cast(call_but_not_bridge_stu  as string)call_but_not_bridge_stu --'天润外呼未接通非OC学生拨打人次（从未接通）',
        ,cast(call_not_bridge_phone_recall_stu as string) call_not_bridge_phone_recall_stu --'天润外呼未接通非OC学生工作手机再拨打人次（从未接通）',
        ,cast(tcr_stu as string) tcr_stu --'天润外呼未接通非OC线索数（从未接通）',
        ,cast(tcr_stu_ph as string) tcr_stu_ph  --'天润外呼未接通非OC工作手机再拨线索数（从未接通）',
        ,cast(sale_commu_class_cnt as string) sale_commu_class_cnt  --销售进入课堂沟通量,
        ,cast(sale_in_lesson_cnt as string)sale_in_lesson_cnt  --销售进入课堂量,
        ,cast(timelength/3600 as string)timelength_hour --销售进入课堂沟通时长小时
        ,cast(call_time_total/3600 +phone_seconds/3600 +class_period/60 as string) tongshi_sum
        ,cast(call_count_total+phone_call_cnt+lp_cnt as string)tongci_sum

    from hf_mobdb.jw_sales_honghei_commu jshc
    left join(
        select stats_date 
            ,user_id 
            ,name
            ,job_number  
            ,role_code   
            ,role_name   --角色 销售或班主任
            ,quarters --岗位   
            ,class  --分类    
            ,department_name --部门
            ,city    
            ,branch  
            ,center  
            ,region  
            ,department  
            ,grp
        from dt_mobdb.dt_charlie_dept_month_end
        )cdme on jshc.user_id=cdme.user_id and substr(jshc.stats_date,1,7)=substr(cdme.stats_date,1,7)

union

    select jhc.stats_date 
        ,jhc.user_id
        ,jhc.name 
        ,jhc.job_number  
        ,'市场'as quarters
        ,wu.third_level as xiaoshou_type
        ,wu.third_level   as branch --三级部门, 销售组别
        ,wu.fourth_level as center  --四级部门,
        ,''as region
        ,wu.fifth_level  as department  --五级部门,
        ,jhc.attend_time 
        ,jhc.call_time_total/3600 as call_time_total_hour
        ,jhc.phone_seconds/3600 as phone_seconds_hour
        ,jhc.class_period/60 as class_period_hour

        ,if(jhc.call_con_count_total is null,0,call_con_count_total) as call_con_count_total--   天润接通次数,
        ,if(jhc.call_count_total is null,0,call_count_total)as call_count_total  --天润拨打次数,
        ,'' phone_call_cnt  -- 工作手机通次,
        ,jhc.phone_bridge_cnt  --工作手机接通通次,

        ,'' call_but_not_bridge_stu  --'天润外呼未接通非OC学生拨打人次（从未接通）',
        ,'' call_not_bridge_phone_recall_stu  --'天润外呼未接通非OC学生工作手机再拨打人次（从未接通）',
        ,'' tcr_stu  --'天润外呼未接通非OC线索数（从未接通）',
        ,'' tcr_stu_ph  --'天润外呼未接通非OC工作手机再拨线索数（从未接通）',
        ,'' sale_commu_class_cnt  --销售进入课堂沟通量,
        ,'' sale_in_lesson_cnt  --销售进入课堂量,
        ,'' timelength_hour --销售进入课堂沟通时长小时
        ,'' tongshi_sum
        ,'' tongci_sum   
    from hf_mobdb.jw_market_honghei_commu jhc
    left join hf_mobdb.will_user_list_crm wu on jhc.user_id=wu.user_id and wu.stats_date=current_date
    where wu.third_level  in ('市场城市化业务部','代理-市场渠道合作中心','代理-市场代理合作中心') 
    and wu.fourth_level in ('城市地推','业务部')
;


insert overwrite table hf_mobdb.sales_tongshi_tongci_rank 

select a1.*
    ,a2.tjs_sum--提交数
    ,a2.yyst_sum --预约试听数
    ,a2.shangtiyanke_sum --上体验课人数
    ,a2.tpsj_sum  --跳票数
    ,a2.tpsx_sum --跳票数-新
    ,a2.sts_sum --试听数
    ,a2.qys_sum --签约数
    ,a2.qyje_sum --签约金额
    ,a2.tpshangtiyanke_sum --跳票上体验课数
    ,a2.wcstshangtiyanke_sum --完成试听上体验课数
    ,case when a2.id ='95491' then '上海升学升学组二组' when a2.id ='95488' then '上海升学升学组一组' else a1.xiaoshou_type end as xiaoshouzubie
    ,case when a2.id ='95491' then '销售组员' when a2.id ='95488' then '销售组员' else a1.quarters end as xiaoshougangwei
from(
    select stats_date 
        ,user_id
        ,name 
        ,job_number  
        ,quarters
        ,xiaoshou_type
        ,branch --三级部门, 销售组别
        ,center  --四级部门,
        ,region
        ,department 
        ,attend_time 
        ,substr(attend_time,1,11) as  attend_time2    
        ,sum(call_time_total_hour) call_time_total_hour_sum --天润通时
        ,sum(phone_seconds_hour) phone_seconds_hour_sum --   工作手机通时,
        ,sum(class_period_hour) class_period_hour_sum--体验课时长
        ,sum(call_con_count_total) call_con_count_total_sum--   天润接通次数,
        ,sum(call_count_total) call_count_total_sum   --天润拨打次数,
        ,sum(phone_call_cnt) phone_call_cnt_sum  -- 工作手机通次,
        ,sum(phone_bridge_cnt)phone_bridge_cnt_sum  --工作手机接通通次,
        ,sum(call_but_not_bridge_stu) call_but_not_bridge_stu_sum --'天润外呼未接通非OC学生拨打人次（从未接通）',
        ,sum(call_not_bridge_phone_recall_stu) call_not_bridge_phone_recall_stu_sum --'天润外呼未接通非OC学生工作手机再拨打人次（从未接通）',
        ,sum(tcr_stu)  tcr_stu_sum--'天润外呼未接通非OC线索数（从未接通）',
        ,sum(tcr_stu_ph) tcr_stu_ph_sum --'天润外呼未接通非OC工作手机再拨线索数（从未接通）',
        ,sum(sale_commu_class_cnt)  sale_commu_class_cnt_sum--销售进入课堂沟通量,
        ,sum(sale_in_lesson_cnt)sale_in_lesson_cnt_sum  --销售进入课堂量,
        ,sum(timelength_hour)timelength_hour_sum --销售进入课堂沟通时长小时
        ,sum(tongshi_sum) tongshi_sum
        ,sum(tongci_sum) tongci_sum
    from hf_mobdb.sales_tongshi_tongci_rank_1
    group by stats_date 
        ,user_id
        ,name 
        ,job_number  
        ,quarters
        ,xiaoshou_type
        ,branch --三级部门, 销售组别
        ,center  --四级部门,
        ,region
        ,department 
        ,attend_time 
        ,substr(attend_time,1,11)
    )a1
left join(
    select s3.sign_time,s3.id 
        ,sum(tjs_sum)as tjs_sum--提交数
        ,sum(yyst_sum) as yyst_sum --预约试听数
        ,sum(shangtiyanke_sum) as shangtiyanke_sum --上体验课人数
        ,sum(tpsj_sum)as tpsj_sum  --跳票数
        ,sum(tpsx_sum) as tpsx_sum --跳票数-新
        ,sum(sts_sum)as sts_sum --试听数
        ,sum(qys_sum)  qys_sum --签约数
        ,sum(qyje_sum)  qyje_sum --签约金额
        ,sum(tpshangtiyanke_sum)  tpshangtiyanke_sum --跳票上体验课数
        ,sum(wcstshangtiyanke_sum) wcstshangtiyanke_sum --完成试听上体验课数
    from hf_mobdb.sales_value_id_contract_3 s3
    group by s3.sign_time,s3.id
    )a2 on a1.user_id=a2.id and a1.attend_time2=a2.sign_time
    ;

