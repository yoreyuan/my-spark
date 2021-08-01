package yore.streaming

import java.sql.{Date, Timestamp}



/**
  * 实时大屏指标 sql
  *
  * datediff 日期间隔
  * ADDDATE(date,INTERVAL expr unit), ADDDATE(expr,days)
  * curdate() 当前日期
  *
  *
  * <b>注意</b>
  *   ① 含有关键字的别名使用反单引包起来(例如 min )
  *   ② left join   表有null  改为 inner join
  *   ③ INTERVAL expr unit 中的数字反单引包起来
  *
  * Created by yore on 2019/4/10 10:32
  */
object indicator_sql {

  case class Word(w: String, c: Long)

  /**
    * b090_cf_direct_repayment_plan样例类
    * @param inte 应还利息
    * @param balance 应还本金
    * @param loan_contract_id 签约合同号
    * @param maturity_date 应还款日期
    * @param term 期数
    */
  case class b090_cf_direct_repayment_plan(
                                            inte: Double,
                                            balance: Double,
                                            loan_contract_id: String,
                                            maturity_date: Date,
                                            term: String)

  /**
    * b090_cf_direct_repayment_record样例类
    * @param loan_contract_id 签约合同号
    * @param term 期数
    * @param rpy_date 还款日期
    */
  case class b090_cf_direct_repayment_record(
                                              loan_contract_id: String,
                                              term: String,
                                              rpy_date: Date)

  /**
    * prplclaim, 测试 b060_prplclaim
    *
    * @param policyno
    */
  case class prplclaim(
                        policyno: String,
                        sumclaim: Double,
                        endcasedate: Date)

  /**
    * t_ec_uw_baseinfo, 测试 b070_t_ec_uw_baseinfo
    *
    * @param policyno
    * @param systemsource
    */
  case class t_ec_uw_baseinfo(
                               policyno: String,
                               systemsource: String,
                               event_time: Timestamp)

  /**
    * key 指标ID
    * value sql
    */
  val SQL = Map(
    // m3+    TODO left join关联后DateDiff计算为空，
    "yqlb2" ->
      """
        |select sum(case when datediff(adddate(curdate(),-1),min_date)>90 then p.inte+p.balance else 0 end)/sum(p.inte+p.balance) c1 from
        |	b090_cf_direct_repayment_plan p left join (
        |		select p.loan_contract_id,min(p.maturity_date) min_date
        |		from b090_cf_direct_repayment_plan p
        |		left join b090_cf_direct_repayment_record r
        |		on p.loan_contract_id=r.loan_contract_id and p.term=r.term and r.rpy_date<curdate()
        |		where p.maturity_date<curdate() -- and r.loan_contract_id is null
        |		group by p.loan_contract_id) `min`
        |	on p.loan_contract_id=`min`.loan_contract_id and p.maturity_date>=`min`.min_date
      """.stripMargin,
    // 逾期金额往前推1个月
    "yqjea1" ->
      """
        |select sum(case when datediff(last_day(adddate(curdate(),interval -1 month)),min_date)>0 then p.inte+p.balance else 0 end) c1 from
        |	b090_cf_direct_repayment_plan p left join (
        |		select p.loan_contract_id,min(p.maturity_date) min_date
        |		from b090_cf_direct_repayment_plan p
        |		left join b090_cf_direct_repayment_record r
        |		on p.loan_contract_id=r.loan_contract_id and p.term=r.term
        |		where p.maturity_date<=last_day(adddate(curdate(),interval -1 month)) and r.loan_contract_id is null
        |		group by p.loan_contract_id) `min`
        |	on p.loan_contract_id=`min`.loan_contract_id and p.maturity_date>=`min`.min_date
      """.stripMargin,
    // 逾期金额往前推2个月 TODO left join关联后DateDiff计算为空，
    "yqjea2" ->
      """
        |select sum(case when datediff(last_day(adddate(curdate(),interval -2 month)),min_date)>0 then p.inte+p.balance else 0 end) c1 from
        |	b090_cf_direct_repayment_plan p left join (
        |		select p.loan_contract_id,min(p.maturity_date) min_date
        |		from b090_cf_direct_repayment_plan p
        |		left join b090_cf_direct_repayment_record r
        |		on p.loan_contract_id=r.loan_contract_id and p.term=r.term
        |		where p.maturity_date<=last_day(adddate(curdate(),interval -2 month)) and r.loan_contract_id is null
        |		group by p.loan_contract_id) `min`
        |on p.loan_contract_id=`min`.loan_contract_id and p.maturity_date>=`min`.min_date
      """.stripMargin,
    // 逾期金额往前推3个月 TODO left join关联后DateDiff计算为空，
    "yqjea3" ->
      """
        |select sum(case when datediff(last_day(adddate(curdate(),interval -3 month)),min_date)>0 then p.inte+p.balance else 0 end) c1
        |from b090_cf_direct_repayment_plan p left join (
        |		select p.loan_contract_id,min(p.maturity_date) min_date
        |		from b090_cf_direct_repayment_plan p
        |		left join b090_cf_direct_repayment_record r
        |		on p.loan_contract_id=r.loan_contract_id and p.term=r.term
        |		where p.maturity_date<=last_day(adddate(curdate(),interval -3 month)) and r.loan_contract_id is null
        |		group by p.loan_contract_id) `min`
        |on p.loan_contract_id=`min`.loan_contract_id and p.maturity_date>=`min`.min_date
      """.stripMargin,
    // 逾期金额往前推4个月 TODO left join关联后DateDiff计算为空，
    "yqjea4" ->
      """
        |select sum(case when datediff(last_day(adddate(curdate(),interval -4 month)),min_date)>0 then p.inte+p.balance else 0 end) c1
        |from b090_cf_direct_repayment_plan p left join (
        |		select p.loan_contract_id,min(p.maturity_date) min_date
        |		from b090_cf_direct_repayment_plan p
        |		left join b090_cf_direct_repayment_record r
        |		on p.loan_contract_id=r.loan_contract_id and p.term=r.term
        |		where p.maturity_date<=last_day(adddate(curdate(),interval -4 month)) and r.loan_contract_id is null
        |		group by p.loan_contract_id) `min`
        |on p.loan_contract_id=`min`.loan_contract_id and p.maturity_date>=`min`.min_date
      """.stripMargin,
    // 逾期金额往前推5个月 TODO left join关联后DateDiff计算为空，
    "yqjea5" ->
      """
        |select sum(case when datediff(last_day(adddate(curdate(),interval -5 month)),min_date)>0 then p.inte+p.balance else 0 end) c1
        |from b090_cf_direct_repayment_plan p left join (
        |		select p.loan_contract_id,min(p.maturity_date) min_date
        |		from b090_cf_direct_repayment_plan p
        |		left join b090_cf_direct_repayment_record r
        |		on p.loan_contract_id=r.loan_contract_id and p.term=r.term
        |		where p.maturity_date<=last_day(adddate(curdate(),interval -5 month)) and r.loan_contract_id is null
        |		group by p.loan_contract_id) `min`
        |on p.loan_contract_id=`min`.loan_contract_id and p.maturity_date>=`min`.min_date
      """.stripMargin,
    // 逾期金额往前推6个月 TODO left join关联后DateDiff计算为空，
    "yqjea6" ->
      """
        |select sum(case when datediff(last_day(adddate(curdate(),interval -6 month)),min_date)>0 then p.inte+p.balance else 0 end) c1
        |from b090_cf_direct_repayment_plan p left join (
        |		select p.loan_contract_id,min(p.maturity_date) min_date
        |		from b090_cf_direct_repayment_plan p
        |		left join b090_cf_direct_repayment_record r
        |		on p.loan_contract_id=r.loan_contract_id and p.term=r.term
        |		where p.maturity_date<=last_day(adddate(curdate(),interval -6 month)) and r.loan_contract_id is null
        |		group by p.loan_contract_id) `min`
        |on p.loan_contract_id=`min`.loan_contract_id and p.maturity_date>=`min`.min_date
      """.stripMargin,
    // 逾期笔数往前推1个月 TODO left join关联后DateDiff计算为空，
    "yqbsa1" ->
      """
        |select count(distinct(case when datediff(last_day(adddate(curdate(),interval -1 month)),min_date)>0 then p.loan_contract_id end)) c1
        |from b090_cf_direct_repayment_plan p left join (
        |		select p.loan_contract_id,min(p.maturity_date) min_date
        |		from b090_cf_direct_repayment_plan p
        |		left join b090_cf_direct_repayment_record r
        |		on p.loan_contract_id=r.loan_contract_id and p.term=r.term
        |		where p.maturity_date<=last_day(adddate(curdate(),interval -1 month)) and r.loan_contract_id is null
        |		group by p.loan_contract_id) `min`
        |on p.loan_contract_id=`min`.loan_contract_id and p.maturity_date>=`min`.min_date
      """.stripMargin,
    // 逾期笔数往前推2个月 TODO left join关联后DateDiff计算为空，
    "yqbsa2" ->
      """
        |select count(distinct(case when datediff(last_day(adddate(curdate(),interval -2 month)),min_date)>0 then p.loan_contract_id end)) c1
        |from b090_cf_direct_repayment_plan p left join (
        |		select p.loan_contract_id,min(p.maturity_date) min_date
        |		from b090_cf_direct_repayment_plan p
        |		left join b090_cf_direct_repayment_record r
        |		on p.loan_contract_id=r.loan_contract_id and p.term=r.term
        |		where p.maturity_date<=last_day(adddate(curdate(),interval -2 month)) and r.loan_contract_id is null
        |		group by p.loan_contract_id) `min`
        |on p.loan_contract_id=`min`.loan_contract_id and p.maturity_date>=`min`.min_date
      """.stripMargin,
    // 逾期笔数往前推3个月 TODO left join关联后DateDiff计算为空，
    "yqbsa3" ->
      """
        |select count(distinct(case when datediff(last_day(adddate(curdate(),interval -3 month)),min_date)>0 then p.loan_contract_id end)) c1
        |from b090_cf_direct_repayment_plan p left join (
        |		select p.loan_contract_id,min(p.maturity_date) min_date
        |		from b090_cf_direct_repayment_plan p
        |		left join b090_cf_direct_repayment_record r
        |		on p.loan_contract_id=r.loan_contract_id and p.term=r.term
        |		where p.maturity_date<=last_day(adddate(curdate(),interval -3 month)) and r.loan_contract_id is null
        |		group by p.loan_contract_id) `min`
        |on p.loan_contract_id=`min`.loan_contract_id and p.maturity_date>=`min`.min_date
      """.stripMargin,
    // 逾期笔数往前推4个月 TODO left join关联后DateDiff计算为空，
    "yqbsa4" ->
      """
        |select count(distinct(case when datediff(last_day(adddate(curdate(),interval -4 month)),min_date)>0 then p.loan_contract_id end)) c1
        |from b090_cf_direct_repayment_plan p left join(
        |		select p.loan_contract_id,min(p.maturity_date) min_date
        |		from b090_cf_direct_repayment_plan p
        |		left join b090_cf_direct_repayment_record r
        |		on p.loan_contract_id=r.loan_contract_id and p.term=r.term
        |		where p.maturity_date<=last_day(adddate(curdate(),interval -4 month)) and r.loan_contract_id is null
        |		group by p.loan_contract_id) `min`
        |on p.loan_contract_id=`min`.loan_contract_id and p.maturity_date>=`min`.min_date
      """.stripMargin,
    // 逾期笔数往前推5个月 TODO left join关联后DateDiff计算为空，
    "yqbsa5" ->
      """
        |select count(distinct(case when datediff(last_day(adddate(curdate(),interval -5 month)),min_date)>0 then p.loan_contract_id end)) c1
        |from b090_cf_direct_repayment_plan p left join (
        |		select p.loan_contract_id,min(p.maturity_date) min_date
        |		from b090_cf_direct_repayment_plan p
        |		left join b090_cf_direct_repayment_record r
        |		on p.loan_contract_id=r.loan_contract_id and p.term=r.term
        |		where p.maturity_date<=last_day(adddate(curdate(),interval -5 month)) and r.loan_contract_id is null
        |		group by p.loan_contract_id) `min`
        |on p.loan_contract_id=`min`.loan_contract_id and p.maturity_date>=`min`.min_date
      """.stripMargin,
    // 逾期笔数往前推6个月 TODO left join关联后DateDiff计算为空，
    "yqbsa6" ->
      """
        |select count(distinct(case when datediff(last_day(adddate(curdate(),interval -6 month)),min_date)>0 then p.loan_contract_id end)) c1
        |from b090_cf_direct_repayment_plan p left join (
        |		select p.loan_contract_id,min(p.maturity_date) min_date
        |		from b090_cf_direct_repayment_plan p
        |		left join b090_cf_direct_repayment_record r
        |		on p.loan_contract_id=r.loan_contract_id and p.term=r.term
        |		where p.maturity_date<=last_day(adddate(curdate(),interval -6 month)) and r.loan_contract_id is null
        |		group by p.loan_contract_id) `min`
        |on p.loan_contract_id=`min`.loan_contract_id and p.maturity_date>=`min`.min_date
      """.stripMargin,
    // 逾期笔数 TODO left join关联后DateDiff计算为空，
    "yqbs" ->
      """
        |select sum(case when datediff(adddate(curdate(),-1),min_date)>0 and datediff(adddate(curdate(),-1),min_date)<=30 then 1 else 0 end) c1
        |from b090_cf_direct_repayment_plan p left join (
        |		select p.loan_contract_id,min(p.maturity_date) min_date
        |		from b090_cf_direct_repayment_plan p
        |		left join b090_cf_direct_repayment_record r
        |		on p.loan_contract_id=r.loan_contract_id and p.term=r.term and r.rpy_date<curdate()
        |		where p.maturity_date<curdate() and r.loan_contract_id is null
        |		group by p.loan_contract_id) `min`
        |on p.loan_contract_id=`min`.loan_contract_id and p.maturity_date>=`min`.min_date
      """.stripMargin,
    // 逾期笔数环比 TODO left join关联后DateDiff计算为空，
    "yqbshb" ->
      """
        |select t1.c1/t2.c1-1 from (
        |	select count(distinct(case when datediff(adddate(curdate(),-1),min_date)>0 then p.loan_contract_id end)) c1
        |	from b090_cf_direct_repayment_plan p left join (
        |		select p.loan_contract_id,min(p.maturity_date) min_date
        |		from b090_cf_direct_repayment_plan p
        |		left join b090_cf_direct_repayment_record r
        |		on p.loan_contract_id=r.loan_contract_id and p.term=r.term and r.rpy_date<curdate()
        |		where p.maturity_date<curdate() and r.loan_contract_id is null
        |		group by p.loan_contract_id) `min`
        |on p.loan_contract_id=`min`.loan_contract_id and p.maturity_date>=`min`.min_date) t1,(
        |		select count(distinct(case when datediff(adddate(curdate(),-1),min_date)>0 then p.loan_contract_id end)) c1
        |		from b090_cf_direct_repayment_plan p left join (
        |			select p.loan_contract_id,min(p.maturity_date) min_date
        |			from b090_cf_direct_repayment_plan p
        |			left join b090_cf_direct_repayment_record r
        |			on p.loan_contract_id=r.loan_contract_id and p.term=r.term and r.rpy_date<adddate(curdate(),-1)
        |			where p.maturity_date<adddate(curdate(),-1) and r.loan_contract_id is null
        |			group by p.loan_contract_id) `min`
        |		on p.loan_contract_id=`min`.loan_contract_id and p.maturity_date>=`min`.min_date) t2
      """.stripMargin,
    // 逾期率（件数）M1 TODO left join关联后DateDiff计算为空，
    "yqlm1a1" ->
      """
        |select count(distinct(case when datediff(last_day(adddate(curdate(),interval -1 month)),min_date)>0
        |and datediff(last_day(adddate(curdate(),interval -1 month)),min_date)<=30  then p.loan_contract_id end)) c1
        |from b090_cf_direct_repayment_plan p left join (
        |		select p.loan_contract_id,min(p.maturity_date) min_date
        |		from b090_cf_direct_repayment_plan p
        |		left join b090_cf_direct_repayment_record r
        |		on p.loan_contract_id=r.loan_contract_id and p.term=r.term
        |		where p.maturity_date<=last_day(adddate(curdate(),interval -1 month)) and r.loan_contract_id is null
        |		group by p.loan_contract_id) `min`
        |on p.loan_contract_id=`min`.loan_contract_id and p.maturity_date>=`min`.min_date
      """.stripMargin,
    // 逾期率（件数）M1 TODO left join关联后DateDiff计算为空，
    "yqlm1a2" ->
      """
        |select count(distinct(case when datediff(last_day(adddate(curdate(),interval -2 month)),min_date)>0
        |and datediff(last_day(adddate(curdate(),interval -2 month)),min_date)<=30  then p.loan_contract_id end)) c1
        |from b090_cf_direct_repayment_plan p left join (
        |		select p.loan_contract_id,min(p.maturity_date) min_date
        |		from b090_cf_direct_repayment_plan p
        |		left join b090_cf_direct_repayment_record r
        |		on p.loan_contract_id=r.loan_contract_id and p.term=r.term
        |		where p.maturity_date<=last_day(adddate(curdate(),interval -2 month)) and r.loan_contract_id is null
        |		group by p.loan_contract_id) `min`
        |on p.loan_contract_id=`min`.loan_contract_id and p.maturity_date>=`min`.min_date
      """.stripMargin,
    // 逾期率（件数）M2 TODO left join关联后DateDiff计算为空，
    "yqlm2a1" ->
      """
        |select count(distinct(case when datediff(last_day(adddate(curdate(),interval -1 month)),min_date)>30
        |and datediff(last_day(adddate(curdate(),interval -1 month)),min_date)<=60  then p.loan_contract_id end)) c1
        |from b090_cf_direct_repayment_plan p left join (
        |		select p.loan_contract_id,min(p.maturity_date) min_date
        |		from b090_cf_direct_repayment_plan p
        |		left join b090_cf_direct_repayment_record r
        |		on p.loan_contract_id=r.loan_contract_id and p.term=r.term
        |		where p.maturity_date<=last_day(adddate(curdate(),interval -1 month)) and r.loan_contract_id is null
        |		group by p.loan_contract_id) `min`
        |on p.loan_contract_id=`min`.loan_contract_id and p.maturity_date>=`min`.min_date
      """.stripMargin,
    // 逾期率（件数）M2 TODO left join关联后DateDiff计算为空，
    "yqlm2a2" ->
      """
        |select count(distinct(case when datediff(last_day(adddate(curdate(),interval -2 month)),min_date)>30
        |and datediff(last_day(adddate(curdate(),interval -2 month)),min_date)<=60  then p.loan_contract_id end)) c1
        |from b090_cf_direct_repayment_plan p left join (
        |		select p.loan_contract_id,min(p.maturity_date) min_date
        |		from b090_cf_direct_repayment_plan p
        |		left join b090_cf_direct_repayment_record r
        |		on p.loan_contract_id=r.loan_contract_id and p.term=r.term
        |		where p.maturity_date<=last_day(adddate(curdate(),interval -2 month)) and r.loan_contract_id is null
        |		group by p.loan_contract_id) `min`
        |on p.loan_contract_id=`min`.loan_contract_id and p.maturity_date>=`min`.min_date
      """.stripMargin,
    // 逾期率（件数）M3 TODO left join关联后DateDiff计算为空，
    "ysqzla1" ->
      """
        |select count(distinct(case when datediff(last_day(adddate(curdate(),interval -1 month)),min_date)>60
        |and datediff(last_day(adddate(curdate(),interval -1 month)),min_date)<=90  then p.loan_contract_id end)) c1
        |from b090_cf_direct_repayment_plan p left join (
        |		select p.loan_contract_id,min(p.maturity_date) min_date
        |		from b090_cf_direct_repayment_plan p
        |		left join b090_cf_direct_repayment_record r
        |		on p.loan_contract_id=r.loan_contract_id and p.term=r.term
        |		where p.maturity_date<=last_day(adddate(curdate(),interval -1 month)) and r.loan_contract_id is null
        |		group by p.loan_contract_id) `min`
        |on p.loan_contract_id=`min`.loan_contract_id and p.maturity_date>=`min`.min_date
      """.stripMargin,
    // 逾期率（件数）M3 TODO left join关联后DateDiff计算为空，
    "ysqzla2" ->
      """
        |select count(distinct(case when datediff(last_day(adddate(curdate(),interval -2 month)),min_date)>60
        |and datediff(last_day(adddate(curdate(),interval -2 month)),min_date)<=90  then p.loan_contract_id end)) c1
        |from b090_cf_direct_repayment_plan p left join (
        |		select p.loan_contract_id,min(p.maturity_date) min_date
        |		from b090_cf_direct_repayment_plan p
        |		left join b090_cf_direct_repayment_record r
        |		on p.loan_contract_id=r.loan_contract_id and p.term=r.term
        |		where p.maturity_date<=last_day(adddate(curdate(),interval -2 month)) and r.loan_contract_id is null
        |		group by p.loan_contract_id) `min`
        |on p.loan_contract_id=`min`.loan_contract_id and p.maturity_date>=`min`.min_date
      """.stripMargin,
    // 逾期率（件数）M3+ TODO left join关联后DateDiff计算为空，
    "bllm3a1" ->
      """
        |select count(distinct(case when datediff(last_day(adddate(curdate(),interval -1 month)),min_date)>90 then p.loan_contract_id end)) c1
        |from b090_cf_direct_repayment_plan p left join (
        |		select p.loan_contract_id,min(p.maturity_date) min_date
        |		from b090_cf_direct_repayment_plan p
        |		left join b090_cf_direct_repayment_record r
        |		on p.loan_contract_id=r.loan_contract_id and p.term=r.term
        |		where p.maturity_date<=last_day(adddate(curdate(),interval -1 month)) and r.loan_contract_id is null
        |		group by p.loan_contract_id) `min`
        |on p.loan_contract_id=`min`.loan_contract_id and p.maturity_date>=`min`.min_date
      """.stripMargin,
    // 逾期率（件数）M3+ TODO left join关联后DateDiff计算为空，
    "bllm3a2" ->
      """
        |select count(distinct(case when datediff(last_day(adddate(curdate(),interval -2 month)),min_date)>90 then p.loan_contract_id end)) c1
        |from b090_cf_direct_repayment_plan p left join (
        |		select p.loan_contract_id,min(p.maturity_date) min_date
        |		from b090_cf_direct_repayment_plan p
        |		left join b090_cf_direct_repayment_record r
        |		on p.loan_contract_id=r.loan_contract_id and p.term=r.term
        |		where p.maturity_date<=last_day(adddate(curdate(),interval -2 month)) and r.loan_contract_id is null
        |		group by p.loan_contract_id) `min`
        |on p.loan_contract_id=`min`.loan_contract_id and p.maturity_date>=`min`.min_date
      """.stripMargin,
    // 已解决金额
    "pfje" ->
      """
        |select sum(sumclaim) c1 from prplclaim c -- cgidb 核心库
        |join t_ec_uw_baseinfo t on t.policyno=c.policyno and t.systemsource='03'  -- policy 寺库库
        |where endcasedate=adddate(curdate(),-1)
      """.stripMargin,
    // 已解决金额环比
    "pfjehb" ->
      """
        |select t1.c1/t2.c1-1 from (
        |	select sum(sumclaim) c1 from prplclaim c -- cgidb 核心库
        |	join t_ec_uw_baseinfo t on t.policyno=c.policyno and t.systemsource='03'  -- policy 寺库库
        |	where endcasedate=adddate(curdate(),-1)) t1,(
        |		select sum(sumclaim) c1 from prplclaim c -- cgidb 核心库
        |		join t_ec_uw_baseinfo t on t.policyno=c.policyno and t.systemsource='03'  -- policy 寺库库
        |		where endcasedate=adddate(curdate(),-2)) t2
      """.stripMargin,
    // 已解决金额累计总额
    "pfjeljze" ->
      """
        |select sum(sumclaim) c1  from prplclaim c -- cgidb 核心库
        |join t_ec_uw_baseinfo t on t.policyno=c.policyno and t.systemsource='03'  -- policy 寺库库
        |where endcasedate<=adddate(curdate(),-1)
      """.stripMargin,
    // 赔付金额往前推1个月 4.4 2019-03-13 \n 3.3 2019-04-14
    "pfjea1" ->
      """
        |select sum(sumclaim) from prplclaim c -- cgidb 核心库
        |join t_ec_uw_baseinfo t on t.policyno=c.policyno and t.systemsource='03' -- policy 寺库库
        |where endcasedate between date_format(adddate(curdate(),INTERVAL -1 month),'%Y-%m-01') and last_day(adddate(curdate(),INTERVAL -1 month))
      """.stripMargin,
    // 赔付金额往前推2个月  4.4 2019-02-13 \n 3.3 2019-04-14
    "pfjea2" ->
      """
        |select sum(sumclaim) from prplclaim c -- cgidb 核心库
        |join t_ec_uw_baseinfo t on t.policyno=c.policyno and t.systemsource='03' -- policy 寺库库
        |where endcasedate between date_format(adddate(curdate(),INTERVAL -2 month),'%Y-%m-01') and last_day(adddate(curdate(),INTERVAL -2 month))
      """.stripMargin,
    // 赔付金额往前推3个月  4.4 2019-01-13 \n 3.3 2019-04-14
    "pfjea3" ->
      """
        |select sum(sumclaim) from prplclaim c -- cgidb 核心库
        |join t_ec_uw_baseinfo t on t.policyno=c.policyno and t.systemsource='03' -- policy 寺库库
        |where endcasedate between date_format(adddate(curdate(),INTERVAL -3 month),'%Y-%m-01') and last_day(adddate(curdate(),INTERVAL -3 month))
      """.stripMargin,
    // 赔付金额往前推4个月   4.5 2018-12-13 \n 3.3 2019-04-14
    "pfjea4" ->
      """
        |select sum(sumclaim) from prplclaim c -- cgidb 核心库
        |join t_ec_uw_baseinfo t on t.policyno=c.policyno and t.systemsource='03' -- policy 寺库库
        |where endcasedate between date_format(adddate(curdate(),INTERVAL -4 month),'%Y-%m-01') and last_day(adddate(curdate(),INTERVAL -4 month))
      """.stripMargin,
    // 赔付金额往前推5个月   4.6 2018-11-13 \n 3.3 2019-04-14
    "pfjea5" ->
      """
        |select sum(sumclaim) from prplclaim c -- cgidb 核心库
        |join t_ec_uw_baseinfo t on t.policyno=c.policyno and t.systemsource='03' -- policy 寺库库
        |where endcasedate between date_format(adddate(curdate(),INTERVAL -5 month),'%Y-%m-01') and last_day(adddate(curdate(),INTERVAL -5 month))
      """.stripMargin,
    // 赔付金额往前推6个月   4.7 2018-10-13 \n 3.3 2019-04-14 必须添加 watermark
    "pfjea6" ->
      """
        |select sum(sumclaim) from prplclaim c -- cgidb 核心库
        |join t_ec_uw_baseinfo t on t.policyno=c.policyno and t.systemsource='03' -- policy 寺库库
        |where endcasedate between date_format(adddate(curdate(),INTERVAL -6 month),'%Y-%m-01') and last_day(adddate(curdate(),INTERVAL -6 month))
      """.stripMargin

  )//.mapValues(v => v.replace("left join", "inner join"))
    .mapValues(v => v.toLowerCase)
    .mapValues(v => {
      var result = v
      for(intervalUnitRegex(interval,expr, unit) <- intervalUnitRegex.findAllIn(v)){
        result = result.replace(s"$interval$expr$unit", s"'$interval$expr$unit'")
        println()
      }
      result.replaceAll("'+", "'")
      // 替换日期格式化方法名(date_format和flink自带的函数有冲突)
        .replace("date_format(", "my_date_format(")
    }).mapValues(v => v.replace("%y-%m-01", "yyyy-MM-01"))
    //TODO 测试环境下需要这样修改下表名，生产环境则不需要。
//    .mapValues(
//      v => v.replace("prplclaim", "b060_prplclaim")
//        .replace("t_ec_uw_baseinfo", "b070_t_ec_uw_baseinfo")
//    )


  /**
    * 目前先仅支持如下：second ,minute,hour ,day ,week ,month
    *
    * INTERVAL expr unit
    */
  val intervalUnitRegex = """(interval[\s]+)([+|-]?[\d]{1}[\s]+)(second|minute|hour|day|week|month)""".r




  def main(args: Array[String]): Unit = {



  }


}
