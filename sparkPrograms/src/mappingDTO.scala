import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object mappingDTO {

  def main(args: Array[String]): Unit = {

    val sparkConfig = new SparkConf()
    sparkConfig.setMaster("local[*]")
               .setAppName("mappingDTO")

    val sparkContext = new SparkContext(sparkConfig)

    val textFileRDD = sparkContext.textFile("C:\\Users\\VC\\Downloads\\auth.csv")

    val mappedRDD = textFileRDD.map(each => {
      val columns = each.split(",")
      AuthDataDTO(columns(0),columns(1),columns(2),columns(3),columns(4),columns(5),columns(6),columns(7),columns(8),columns(9),columns(10),columns(11),columns(12),columns(13),columns(14),columns(15),columns(16),columns(17),columns(18),columns(19),columns(20),columns(21),columns(22),columns(23),columns(24),columns(25),columns(26),columns(27),columns(28),columns(29),columns(30),columns(31),columns(31),columns(32),columns(33),columns(34),columns(35),columns(36),columns(37),columns(38),columns(39),columns(40),columns(41),columns(42),columns(43),columns(44),columns(45),columns(46),columns(47),columns(48),columns(49),columns(50),columns(51),columns(52),columns(53),columns(54),columns(55),columns(56),columns(57),columns(58),columns(59),columns(60),columns(61),columns(62),columns(63),columns(64),columns(65),columns(66),columns(67),columns(68),columns(69),columns(70),columns(71),columns(72),columns(73),columns(74),columns(75),columns(76),columns(77),columns(78),columns(79),columns(80),columns(81),columns(82),columns(83),columns(84),columns(85),columns(86),columns(87),columns(88),columns(89),columns(90),columns(91),columns(92),columns(93),columns(94),columns(95),columns(96),columns(97),columns(98),columns(99),columns(100),columns(101),columns(102),columns(103),columns(104),columns(105),columns(106),columns(107),columns(108),columns(109),columns(110),columns(111),columns(112),columns(113),columns(114),columns(115),columns(116),columns(117),columns(118),columns(119),columns(120),columns(121),columns(122),columns(123),columns(124),columns(125),columns(126),columns(127),columns(128),columns(129),columns(130),columns(131),columns(131),columns(132),columns(133),columns(134),columns(135),columns(136),columns(137),columns(138),columns(139),columns(140),columns(141),columns(142),columns(143),columns(144),columns(145),columns(146))
    })

    mappedRDD.foreach(each => println(each))

    sparkContext.stop()

  }

  case class AuthDataDTO(auth_code:String,subreq_id:String,aua:String,sa:String,asa:String,ver:String,tid:String,license_id:String,req_datetime:String,tkn_used_flag:String,tkn_type:String,pid_ts:String,pid_ver:String,pi_uses_flag:String,pa_uses_flag:String,pfa_uses_flag:String,bio_uses_flag:String,bt_fmr_uses_flag:String,bt_fir_uses_flag:String,bt_iir_uses_flag:String,pin_uses_flag:String,otp_uses_flag:String,pi_used_flag:String,pa_used_flag:String,pfa_used_flag:String,bio_used_flag:String,bt_fmr_used_flag:String,bt_fir_used_flag:String,bt_iir_used_flag:String,pin_used_flag:String,otp_used_flag:String,otp_identifier:String,lang:String,pi_name_used_flag:String,pi_ms:String,pi_mv:String,pi_match_score:String,pi_lname_used_flag:String,pi_lname_ms:String,pi_lname_mv:String,pi_lname_match_score:String,pi_phone_used_flag:String,pi_email_used_flag:String,pi_gender_used_flag:String,pi_gender:String,pi_dob_used_flag:String,pi_dob:String,pi_dobt_used_flag:String,pi_dobt:String,pi_age_used_flag:String,pi_age:String,pfa_addr_used_flag:String,pfa_addr_ms:String,pfa_addr_mv:String,pfa_match_score:String,pfa_laddr_used_flag:String,pfa_laddr_ms:String,pfa_laddr_mv:String,pfa_laddr_match_score:String,pa_ms:String,pa_co_used_flag:String,pa_house_used_flag:String,pa_street_used_flag:String,pa_lm_used_flag:String,pa_loc_used_flag:String,pa_vtc_used_flag:String,pa_vtc:String,pa_po_used_flag:String,pa_po:String,pa_subdist_used_flag:String,pa_subdist:String,pa_dist_used_flag:String,pa_dist:String,pa_state_used_flag:String,pa_state:String,pa_pc_used_flag:String,pa_pc:String,fmr_count:String,fir_count:String,iir_count:String,finger_match_score:String,uidai_tfmr:String,aua_tfmr:String,finger_match_threshold:String,fmr_gall_type:String,fmr_gall_vendor:String,fmr_sdk_vendor:String,fmr_sdk_version:String,fir_gall_type:String,fir_gall_vendor:String,fir_sdk_vendor:String,fir_sdk_version:String,iir_gall_type:String,iir_gall_vendor:String,iir_sdk_vendor:String,iir_sdk_version:String,auth_result:String,error_code:String,error_classfn:String,resp_datetime:String,auth_duration:String,fdc:String,idc:String,udc:String,locn_lat:String,locn_lng:String,locn_vtc_code:String,locn_subdist_code:String,locn_dist_code:String, locn_state_code:String,locn_pc:String, enr_ref_id:String,res_gender:String,res_birth_day:String,res_birth_month:String,res_birth_year:String,res_dob:String,res_dobt:String,res_age:String,res_pincode:String,res_vtc_code:String,res_vtc_name:String,res_po_name:String,res_subdist_code:String,res_subdist_name:String,res_dist_code:String,res_dist_name:String,res_state_code:String,res_state_name:String,uid_gen_date:String,txn:String,auth_type:String,hash_uid:String,locn_alt:String,lot:String,bfd_done_flag:String,finger_matching_type:String,finger_fusion_perfomed:String,iris_match_score:String,iris_threshold:String,iris_matching_type:String,iris_fusion_perfomed:String,auth_xml_size:String,pid_size:String,data_type:String,skey_scheme:String,ssk_type:String,ki:String,kyc_flag:String)

}

