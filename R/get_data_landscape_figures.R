get_data_landscape_figures <- function(toxval.db, save_png=FALSE){
  # palette considerations
  # https://stackoverflow.com/questions/9563711/r-color-palettes-for-many-data-classes
  if(save_png){
    png_width = 12
    png_height = 7
    png_units = "in"
  }

  # Parameter to set font size consistently
  global_font_size = 16

  # Empty list to store figures
  dl_fig_list = list()

  outputDir <- paste0(toxval.config()$datapath, "release_files/", toxval.db, "_figures/")
  if(!dir.exists(outputDir)) dir.create(outputDir, recursive = TRUE)

  chem_class <- readxl::read_xlsx(paste0(toxval.config()$datapath, "release_files/chemical classes 2024-04-25.xlsx")) %>%
    dplyr::select(dtxsid, class_type,chosen_class)

  message("Pulling data for figures...")
  toxval_dbv <- runQuery("select * from toxval", toxval.db) %>%
    dplyr::filter(!grepl("fail", qc_status))

  filt_type <- toxval_dbv %>%
    dplyr::mutate(toxval_type2 = dplyr::case_when(
      grepl("AEGL 1", toxval_type, ignore.case=TRUE) ~ "AEGL 1",
      grepl("AEGL 2", toxval_type, ignore.case=TRUE) ~ "AEGL 2",
      grepl("AEGL 3", toxval_type, ignore.case=TRUE) ~ "AEGL 3",
      TRUE ~ toxval_type)) %>%
    dplyr::count(toxval_type2) %>%
    dplyr::filter(n>100)

  sc <- runQuery(paste0("SELECT distinct b.toxval_type, e.toxval_type_supercategory ",
                        "FROM toxval b ",
                        "LEFT JOIN toxval_type_dictionary e on b.toxval_type=e.toxval_type"),
                 toxval.db)

  sp <- runQuery(paste0("SELECT distinct b.species_id, d.common_name ",
                        "FROM toxval b ",
                        "LEFT JOIN species d on b.species_id=d.species_id"),
                 toxval.db)

  tvdb <- toxval_dbv %>%
    dplyr::left_join(sc,
                     by="toxval_type") %>%
    dplyr::left_join(sp,
                     by="species_id")%>%
    dplyr::mutate(
      species = dplyr::case_when(common_name=="House Mouse" ~ "Mouse",
                                 common_name=="Western European House Mouse" ~ "Mouse",
                                 common_name=="European Rabbit" ~ "Rabbit",
                                 common_name=="Japanese Quail" ~ "Quail",
                                 TRUE ~ common_name),
      toxval_type_supercategory_abbrev = dplyr::case_when(
        # toxval_type_supercategory == "Toxicity Value" ~ "",
        toxval_type_supercategory == "Dose Response Summary Value" ~ "DRSV",
        # toxval_type_supercategory == "Media Exposure Guidelines" ~ "MEG",
        toxval_type_supercategory == "Mortality Response Summary Value" ~ "MRSV",
        # toxval_type_supercategory == "Acute Exposure Guidelines" ~ "AEG",
        TRUE ~ toxval_type_supercategory
      )
    )

  ### Figure stacked bar by source----
  # stacked bar supercategory by source
  source_n <- tvdb %>%
    dplyr::count(source, name = "src_rec_n")

  source_nchem <- tvdb %>%
    dplyr::select(dtxsid,source,toxval_type_supercategory)%>%
    dplyr::distinct() %>%
    dplyr::count(source, name = "src_chem_n")

  tvdb <- tvdb %>%
    dplyr::left_join(source_n,
                     by="source") %>%
    dplyr::left_join(source_nchem,
                     by = "source") %>%
    dplyr::mutate(
      source_name_rec = dplyr::case_when(
        src_rec_n < 3000 ~ "Other",
        TRUE ~ source),
      source_name_chem = dplyr::case_when(
        src_chem_n < 500 ~ "Other",
        TRUE ~ source)
    )

  # Consistent palette colors across plots
  # https://stackoverflow.com/questions/53506536/need-specific-coloring-in-ggplot2-with-viridis
  # Named vector of colors by source_name, add "Other"
  source_name_fill_pal <- viridisLite::viridis(length(unique(c(tvdb$source_name_rec, tvdb$source_name_chem))),
                                               option = "H") %T>%
    { names(.) <- unique(c(tvdb$source_name_rec, tvdb$source_name_chem)) }

  species_fill_pal <- viridisLite::viridis(length(unique(tvdb$species)),
                                           option = "H") %T>%
    { names(.) <- unique(tvdb$species) }

  message("Generating Plots...")

  # Fig 2
  dl_fig_list[["Record Count by Effect Type Supercategory"]] =
    tvdb %>%
    dplyr::filter(!is.na(toxval_type_supercategory)) %>%
    ggplot2::ggplot(ggplot2::aes(toxval_type_supercategory_abbrev,
                                 fill=source_name_rec,
                                 label=source_name_rec)) +
    ggplot2::geom_bar() +
    ggplot2::theme_minimal(base_size = global_font_size) +
    # ggplot2::theme(text = ggplot2::element_text(size = 14)) +
    ggplot2::guides(fill=ggplot2::guide_legend(ncol=2)) +
    ggplot2::theme(axis.text.x = ggplot2::element_text(angle=45,
                                                       vjust=1,
                                                       hjust=1,
                                                       size=global_font_size),
                   axis.text.y = ggplot2::element_text(size = global_font_size),
                   text = ggplot2::element_text(size = global_font_size)) +
    ggplot2::labs(x = "Effect Type Supercategory",
                  y = "Record Count",
                  fill = "Source Name") +
    ggplot2::scale_fill_manual(values=source_name_fill_pal, drop=TRUE) +
    # ggplot2::scale_fill_viridis_d(option="H") +
    # ggplot2::scale_fill_manual(values=unname(pals::trubetskoy())) +
    # ggplot2::scale_fill_manual(values = dichromat::dichromat(viridisLite::turbo(12), type = "protan")) +
    ggplot2::scale_y_continuous(label = scales::comma)

  sources_chem_collapse <- tvdb %>%
    dplyr::select(source_name_chem, toxval_type_supercategory, toxval_type_supercategory_abbrev, dtxsid) %>%
    dplyr::distinct()

  # Optional Panel B
  dl_fig_list[["Chemical Count by Effect Type Supercategory"]] =
    sources_chem_collapse %>%
    ggplot2::ggplot(ggplot2::aes(toxval_type_supercategory_abbrev,
                                 fill=source_name_chem,
                                 label=source_name_chem)) +
    ggplot2::geom_bar() +
    ggplot2::theme_minimal(base_size = global_font_size) +
    ggplot2::guides(fill=ggplot2::guide_legend(ncol=2)) +
    ggplot2::theme(axis.text.x = ggplot2::element_text(angle=45,
                                                       vjust=1,
                                                       hjust=1,
                                                       size=global_font_size),
                   axis.text.y = ggplot2::element_text(size = global_font_size),
                   text = ggplot2::element_text(size = global_font_size)) +
    ggplot2::labs(x = "Effect Type Supercategory",
                  y = "Chemical Count",
                  fill = "Source Name") +
    ggplot2::scale_fill_manual(values=source_name_fill_pal) +
    # ggplot2::scale_fill_viridis_d(option="H") +
    # ggplot2::scale_fill_manual(values=unname(pals::trubetskoy())) +
    ggplot2::scale_y_continuous(label = scales::comma)

  dl_fig_list[["Record-Chem Count Combined"]] = cowplot::plot_grid(
    dl_fig_list$`Record Count by Effect Type Supercategory`,
    dl_fig_list$`Chemical Count by Effect Type Supercategory`,
    labels = "AUTO"
  )

  ### Figure supercategory effect type panel ----
  toxvaltypes <- tvdb %>%
    dplyr::select(toxval_type)%>%
    dplyr::distinct()

  effect_types <- tvdb %>%
    dplyr::mutate(effect_type =
                    dplyr::case_when(grepl("BMD",ignore.case=TRUE,toxval_type)~"BMD",
                                     grepl("LD",ignore.case=TRUE,toxval_type)~"LD",
                                     grepl("NOAEL",ignore.case=TRUE,toxval_type)~"N(OA)EL",
                                     grepl("LOAEL",ignore.case=TRUE,toxval_type)~"L(OA)EL",
                                     grepl("\\bNEL",ignore.case=TRUE,toxval_type)~"N(OA)EL",
                                     grepl("LEL",ignore.case=TRUE,toxval_type)~"L(OA)EL",
                                     grepl("LCL",ignore.case=TRUE,toxval_type)~"L(OA)EL",
                                     grepl("BMC",ignore.case=TRUE,toxval_type)~"BMC",
                                     grepl("LOEL",ignore.case=TRUE,toxval_type)~"L(OA)EL",
                                     grepl("NOEL",ignore.case=TRUE,toxval_type)~"N(OA)EL",
                                     grepl("AEGL",ignore.case=TRUE,toxval_type)~"AEGL",
                                     grepl("FEL",ignore.case=TRUE,toxval_type)~"FEL",
                                     grepl("\\bLC",ignore.case=TRUE,toxval_type)~"LC",
                                     grepl("cancer slope",ignore.case=TRUE,toxval_type)~"cancer slope factor",
                                     grepl("cancer unit",ignore.case=TRUE,toxval_type)~"cancer unit risk",
                                     grepl("HHBP",ignore.case=TRUE,toxval_type)~"HHBP",
                                     grepl("RfD",ignore.case=TRUE,toxval_type)~"RfD",
                                     grepl("RfC",ignore.case=TRUE,toxval_type)~"RfC",
                                     grepl("Effective Response",ignore.case=TRUE,toxval_type)~"BMD",
                                     grepl("LEC",ignore.case=TRUE,toxval_type)~"L(OA)EC",
                                     grepl("LOEC",ignore.case=TRUE,toxval_type)~"L(OA)EC",
                                     grepl("LOAEC",ignore.case=TRUE,toxval_type)~"L(OA)EC",
                                     grepl("NEC",ignore.case=TRUE,toxval_type)~"N(OA)EC",
                                     grepl("NOEC",ignore.case=TRUE,toxval_type)~"N(OA)EC",
                                     grepl("NOAEC",ignore.case=TRUE,toxval_type)~"N(OA)EC",
                                     grepl("DNEL",ignore.case=TRUE,toxval_type)~"DNEL",
                                     grepl("reference exposure",ignore.case=TRUE,toxval_type)~"REL",
                                     grepl("level of distinct odor",ignore.case=TRUE,toxval_type)~"LOA",
                                     grepl("PAC-",ignore.case=TRUE,toxval_type)~"PAC",
                                     grepl("screening level ",ignore.case=TRUE,toxval_type)~"RSL",
                                     grepl("SSL",ignore.case=TRUE,toxval_type)~"RSL",
                                     TRUE~toxval_type))

  toxvaltypes_collapse <- effect_types %>%
    dplyr::select(effect_type, toxval_type) %>%
    dplyr::distinct()

  effect_types_n = effect_types %>%
    dplyr::count(toxval_type_supercategory_abbrev,
                 name = "n_total")

  # Plot supercategory and effect_type
  effect_types_p = effect_types %>%
    dplyr::count(toxval_type_supercategory_abbrev, effect_type) %>%
    dplyr::group_by(toxval_type_supercategory_abbrev) %>%
    dplyr::mutate(pct = prop.table(n) * 100,
                  effect_type_c = dplyr::case_when(
                    pct < 10 ~ "Other",
                    TRUE ~ effect_type
                  )
    ) %>%
    dplyr::group_by(toxval_type_supercategory_abbrev, effect_type_c) %>%
    dplyr::ungroup() %>%
    dplyr::distinct()

  # Get grouped list of effects with proportions to use as caption
  effects_other_caption = effect_types_p %>%
    dplyr::left_join(effect_types_n,
                     by = "toxval_type_supercategory_abbrev") %>%
    # dplyr::filter(effect_type_c == "Other") %>%
    dplyr::group_by(toxval_type_supercategory_abbrev) %>%
    dplyr::arrange(dplyr::desc(n)) %>%
    dplyr::mutate(effect_type_caption = effect_type %>%
                    paste0("*", ., "*",
                           " (", format(n, big.mark=","),
                           "; ", round(pct, 2), "%)") %>%
                    toString() %>%
                    paste0("**", toxval_type_supercategory_abbrev, "**",
                           " (N=", format(n_total, big.mark=","),
                           ") Effect Type (N; %): ",
                           ., ".") %>%
                    stringr::str_squish() %>%
                    gsub("( ", "(", ., fixed = TRUE)
    ) %>%
    dplyr::pull(effect_type_caption) %>%
    unique() %>%
    paste0(collapse = "\n\n\n")

  effect_types_p <- effect_types_p %>%
    dplyr::select(toxval_type_supercategory_abbrev, pct, n, effect_type = effect_type_c) %>%
    group_by(toxval_type_supercategory_abbrev, effect_type) %>%
    dplyr::summarise(n = sum(n),
                     pct = sum(pct)) %>%
    dplyr::mutate(
      pct_label = dplyr::case_when(
        # Arbitrary 10% cutoff for label display
        pct < 10 ~ "",
        TRUE ~ paste0(effect_type, "\n (", sprintf("%1.1f", pct),"%)")
      )
    )

  # https://stackoverflow.com/questions/62800823/set-a-color-to-show-clear-the-numbers/62801025#62801025
  # Trick from scales::show_col
  hcl <- farver::decode_colour(viridisLite::turbo(length(unique(effect_types_p$effect_type))), "rgb", "hcl")
  # hcl <- farver::decode_colour(pals::trubetskoy(length(unique(effect_types_p$effect_type))), "rgb", "hcl")

  label_col <- ifelse(hcl[, "l"] > 50, "black", "white")

  dl_fig_list[["Effect Type Supercategory Bar Plot"]] =
    effect_types_p %>%
    dplyr::arrange(toxval_type_supercategory_abbrev) %>%
    ggplot2::ggplot() +
    ggplot2::aes(
      x=toxval_type_supercategory_abbrev,
      y=pct,
      group = toxval_type_supercategory_abbrev,
      fill = effect_type) +
    ggplot2::geom_bar(stat="identity") +
    ggplot2::coord_flip() +
    ggplot2::theme_minimal(base_size = global_font_size) +
    ggplot2::scale_fill_viridis_d(option="H") +
    # ggplot2::scale_fill_manual(values=unname(pals::trubetskoy())) +
    # ggplot2::scale_fill_brewer(palette="Set3") +
    ggplot2::ylab("") +
    ggplot2::xlab("") +
    ggplot2::guides(fill="none") +
    ggplot2::theme(axis.text.x=ggplot2::element_blank(),
                   axis.ticks.x=ggplot2::element_blank(),
                   text=ggplot2::element_text(size=global_font_size)) +
    ggplot2::geom_text(ggplot2::aes(label=pct_label,
                                    color = effect_type),
                       position=ggplot2::position_stack(vjust=0.5),
                       show.legend = FALSE,
                       fontface = "bold") +
    ggplot2::scale_color_manual(values = unname(label_col))

  ### Figure panel DRSV----
  species_n <- tvdb %>%
    dplyr::filter(toxval_type_supercategory=="Dose Response Summary Value") %>%
    dplyr::count(species)

  speciesn <- species_n %>%
    dplyr::left_join(tvdb,
                     by="species")

  #### Species by Study Type and Effect Type Category Counts----
  dl_fig_list[["Species by Study Type and Effect Type Category Counts"]] =
    speciesn %>%
    dplyr::filter(toxval_type_supercategory=="Dose Response Summary Value") %>%
    dplyr::filter(study_type%in% c("acute","chronic","short-term","subchronic",
                                   "developmental","reproduction developmental")) %>%
    dplyr::filter(n > 1000) %>%
    ggplot2::ggplot(ggplot2::aes(x=study_type,fill=species)) +
    ggplot2::geom_bar() +
    ggplot2::theme_minimal(base_size = global_font_size) +
    ggplot2::scale_fill_viridis_d(option="H") +
    # ggplot2::scale_fill_manual(values=species_fill_pal) +
    # ggplot2::scale_fill_manual(values=unname(pals::trubetskoy())) +
    ggplot2::labs(y="Record Count",
                  x="Study Type",
                  fill="Species") +
    ggplot2::guides(fill=ggplot2::guide_legend(ncol=1)) +
    ggplot2::theme(axis.text.x = ggplot2::element_text(angle=45,
                                                       vjust=1,
                                                       hjust=1,
                                                       size=global_font_size),
                   axis.text.y = ggplot2::element_text(size = global_font_size),
                   text = ggplot2::element_text(size = global_font_size)) +
    ggplot2::scale_y_continuous(label = scales::comma)

  # #### Species by Study Type and Effect Type Category DRSV Box Plots----
  # dl_fig_list[["Species by Study Type and Effect Type Category DRSV Box Plots"]] =
  #   speciesn %>%
  #   dplyr::filter(toxval_type_supercategory=="Dose Response Summary Value") %>%
  #   dplyr::filter(study_type%in% c("acute","chronic","short-term","subchronic",
  #                                  "developmental","reproduction developmental")) %>%
  #   dplyr::filter(n>1000) %>%
  #   ggplot2::ggplot(ggplot2::aes(x=study_type,y=log10(toxval_numeric),fill=species)) +
  #   ggplot2::geom_boxplot(outlier.colour="black",
  #                         outlier.shape=21,outlier.size=1,outlier.fill="white",
  #                         notch=FALSE) +
  #   # ggplot2::geom_jitter(aes(color=class_type),size=0.6,alpha = 0.9) +
  #   # ggplot2::scale_y_continuous(trans="log10",limits=c(1e-5,1000)) +
  #   # ggplot2::scale_fill_manual(values=c("white","red")) +
  #   ggplot2::coord_flip(ylim = c(-5,5)) +
  #   ggplot2::theme_bw() +
  #   ggplot2::labs(y="DRSV log10(mg/kg-day)",
  #                 x="",
  #                 fill="Species") +
  #
  #   ggplot2::scale_fill_viridis_d(option="H") +
  #   # ggplot2::scale_fill_manual(values=species_fill_pal) +
  #   # ggplot2::scale_fill_manual(values=unname(pals::trubetskoy())) +
  #   ggplot2::theme(text=ggplot2::element_text(size=global_font_size))

  effect_type_g = effect_types %>%
    dplyr::filter(toxval_type_supercategory=="Dose Response Summary Value",
                  study_type %in% c("acute","chronic","short-term","subchronic",
                                    "developmental","reproduction developmental"),
                  toxval_units %in% c("mg/kg-day")) %>%
    dplyr::select(effect_type, study_type, toxval_numeric)

  effect_type_g_pal <- viridisLite::viridis(length(unique(effect_type_g$effect_type)),
                                            option = "H") %T>%
    { names(.) <- unique(effect_type_g$effect_type) }

  # Manually select colors
  effect_type_g_pal = c("BMD"="#4454C4FF",
                        "N(OA)EL"="#A2FC3CFF",
                        "L(OA)EL"="#FABA39FF")

  #### Species by Study Type and Effect Type Category DRSV Box Plots----
  dl_fig_list[["Effect Type Category DRSV Effect Type Box Plots"]] =
    effect_type_g %>%
    dplyr::filter(!effect_type %in% c("N(OA)EC", "L(OA)EC")) %>%
    dplyr::group_by(effect_type) %>%
    # Filter out small groups
    dplyr::filter(dplyr::n()>100) %>%
    dplyr::ungroup() %>%
    ggplot2::ggplot(ggplot2::aes(x=study_type,y=log10(toxval_numeric), fill=effect_type)) +
    ggplot2::geom_boxplot(outlier.colour="black",
                          outlier.shape=21,outlier.size=1,outlier.fill="white",
                          notch=FALSE) +
    # ggplot2::geom_jitter(aes(color=class_type),size=0.6,alpha = 0.9) +
    # ggplot2::scale_y_continuous(trans="log10",limits=c(1e-5,1000)) +
    # ggplot2::scale_fill_manual(values=c("white","red")) +
    ggplot2::coord_flip(ylim = c(-5,5)) +
    ggplot2::theme_bw() +
    ggplot2::labs(y="DRSV log10(mg/kg-day)",
                  x="",
                  fill="Effect Type") +

    # ggplot2::scale_fill_viridis_d(option="H") +
    ggplot2::scale_fill_manual(values=effect_type_g_pal, drop=TRUE) +
    # ggplot2::scale_fill_manual(values=species_fill_pal) +
    # ggplot2::scale_fill_manual(values=unname(pals::trubetskoy())) +
    ggplot2::theme(text=ggplot2::element_text(size=global_font_size))

  dl_fig_list[["Species Plots Combined"]] = cowplot::plot_grid(
    dl_fig_list$`Species by Study Type and Effect Type Category Counts`,
    dl_fig_list$`Effect Type Category DRSV Effect Type Box Plots`,
    labels = "AUTO"
  )

  ### Boxplots by chem_class ----
  toxval_dbv_chem <- toxval_dbv %>%
    dplyr::select(dtxsid) %>%
    dplyr::distinct()

  class_count <- toxval_dbv_chem %>%
    dplyr::left_join(chem_class,
                     by='dtxsid') %>%
    dplyr::select(dtxsid)

  oral <- tvdb %>%
    dplyr::filter(toxval_type_supercategory=="Dose Response Summary Value") %>%
    dplyr::filter(exposure_route=="oral") %>%
    dplyr::filter(toxval_units=="mg/kg-day")

  # get median across all PODs
  m <- chem_class %>%
    dplyr::left_join(oral,
                     by="dtxsid") %>%
    dplyr::filter(!is.na(toxval_numeric)) %>%
    dplyr::summarise(med = stats::median(log10(toxval_numeric)))

  form <- chem_class %>%
    dplyr::left_join(oral,
                     by="dtxsid") %>%
    dplyr::filter(!is.na(toxval_numeric)) %>%
    dplyr::group_by(chosen_class) %>%
    dplyr::mutate(count = dplyr::n_distinct(dtxsid)) %>%
    dplyr::filter(count>2)

  # Get log10 median for all chemicals
  v_med_line = stats::median(log10(form$toxval_numeric))
  # Get 50 chemicals with lowest median log10 value
  list <- form %>%
    dplyr::group_by(chosen_class) %>%
    dplyr::summarise(med = stats::median(log10(toxval_numeric))) %>%
    # Select lowest 50 chemicals
    dplyr::arrange(med, chosen_class) %>%
    head(50)
    # dplyr::filter(med<0.875)

  fform <- list %>%
    dplyr::left_join(form,
                     by="chosen_class") %>%
    dplyr::group_by(chosen_class) %>%
    dplyr::ungroup()
  # fform %>%
  #   dplyr::select(chosen_class,class_type) %>%
  #   dplyr::distinct() %>%
  #   dplyr::group_by(class_type) %>%
  #   dplyr::tally()

  chem_class_pal <- viridisLite::viridis(length(unique(fform$class_type)),
                                         option = "H") %T>%
    { names(.) <- unique(fform$class_type) }

  # Manually select colors
  chem_class_pal = c(
    "Drug"="#4454C4FF",
    "ClassyFire"="#28BBECFF",
    "Expert"="#A2FC3CFF",
    "PFAS"="#FB8022FF",
    "Pesticide"="#7A0403FF"
  )

  dl_fig_list[["Chemical Class Boxplots"]] =
    fform %>%
    ggplot2::ggplot(ggplot2::aes(x=stats::reorder(chosen_class,
                                                  log10(toxval_numeric),
                                                  median,na.rm=TRUE),
                                 y=log10(toxval_numeric),
                                 fill=class_type)) +
    ggplot2::geom_boxplot(outlier.colour="black",
                          outlier.shape=21,
                          outlier.size=2,
                          outlier.fill="white",
                          notch=FALSE) +
    # geom_jitter(aes(color=class_type),size=0.6,alpha = 0.9) +
    ggplot2::geom_hline(yintercept=v_med_line, color="red", linewidth=1) +
    # scale_y_continuous(trans="log10",limits=c(1e-5,1000)) +
    # scale_fill_manual(values=c("white","red")) +
    # ggplot2::scale_fill_manual(values=unname(pals::trubetskoy())) +
    # ggplot2::scale_fill_viridis_d(option="H") +
    ggplot2::scale_fill_manual(values=chem_class_pal, drop=TRUE) +
    ggplot2::coord_flip() +
    ggplot2::theme_bw(base_size = global_font_size) +
    ggplot2::labs(y="DRSV log10(mg/kg-day)",
                  x="",
                  fill = "Class Type") +
    ggplot2::theme(text=ggplot2::element_text(size=global_font_size),
                   legend.position="bottom")

  chem_class_cap = fform %>%
    dplyr::select(class_type, chosen_class) %>%
    dplyr::distinct() %>%
    dplyr::count(class_type) %>%
    dplyr::mutate(class_type = dplyr::case_when(
      class_type == "PFAS" ~ "PFAS (OECD definition)",
      class_type == "Drug" ~ "drugs",
      class_type == "Pesticide" ~ "pesticides",
      class_type == "Expert" ~ "as expert classed",
      class_type == "ClassyFire" ~ "categorized in ClassyFire",
      TRUE ~ class_type
    )) %>%
    dplyr::arrange(n)

  # # Break/testing additional layers
  #
  # ggplot2::geom_text(ggplot2::aes(x = chosen_class,
  #                                 y=count,
  #                                 label = count,
  #                                 group=chosen_class),
  #                    size=3)
  #
  # ggplot2::theme(axis.text=ggplot2::element_text(size=10),
  #                axis.title=ggplot2::element_text(size=14),
  #                plot.title=ggplot2::element_text(size=global_font_size,
  #                                                 vjust=0.5,
  #                                                 hjust=0.5),
  #                strip.text.x = ggplot2::element_text(size = 10),
  #                plot.margin = ggplot2::margin(t=20,
  #                                              r=20,
  #                                              b=50,
  #                                              l=20)) +
  #   ggplot2::geom_text(
  #     ggplot2::aes(x = name, y = count, label = count, group = week),
  #     position = ggplot2::position_dodge(width = 1),
  #     vjust = -0.5, size = 2
  #   )

  ### Density plots----
  dl_fig_list[["Oral-Inhalation Density Plots"]] =
    tvdb %>%
    dplyr::filter(toxval_type_supercategory %in% c("Dose Response Summary Value",
                                                   "Mortality Response Summary Value")) %>%
    # dplyr::filter(exposure_route=="oral") %>%
    dplyr::filter(toxval_units %in% c("mg/kg-day","mg/kg","mg/m3"),
                  study_type %in% c("acute", "short-term", "subchronic",
                                    "chronic", "reproduction developmental",
                                    "developmental")) %>%
    ggplot2::ggplot(ggplot2::aes(x=log10(toxval_numeric),
                                 color = study_type,
                                 ggplot2::after_stat(density))) +
    # ggplot2::stat_density(ggplot2::aes(x = log10(toxval_numeric),
    #                                    ggplot2::after_stat(density)),
    #                       bw = 1) +
    ggplot2::geom_histogram(color="black",
                            # fill="transparent",
                            alpha=0.5
    ) +
    ggplot2::geom_line(stat = "density",
                       linewidth = 1) +

    ggplot2::scale_x_continuous(limits=c(-4,8)) +
    ggplot2::scale_color_viridis_d(option="H") +
    ggplot2::theme_bw() +
    ggplot2::xlab(paste0("log10(effect level)")) +

    ggplot2::labs(x = "log10(effect level)",
                  color = "Study Type") +
    ggplot2::facet_wrap(ggplot2::vars(toxval_units)) +
    ggplot2::theme(axis.text=ggplot2::element_text(size=global_font_size),
                   axis.title=ggplot2::element_text(size=global_font_size),
                   plot.title=ggplot2::element_text(size=global_font_size,face="bold",vjust=0.5,hjust=0.5),
                   strip.text.x = ggplot2::element_text(size = global_font_size),
                   plot.margin = ggplot2::margin(t=20,r=20,b=50,l=20),
                   legend.position="bottom",
                   legend.text=ggplot2::element_text(size=global_font_size))

  density_percentiles = tvdb %>%
    dplyr::filter(toxval_type_supercategory %in% c("Dose Response Summary Value",
                                                   "Mortality Response Summary Value")) %>%
    # dplyr::filter(exposure_route=="oral") %>%
    dplyr::filter(toxval_units %in% c("mg/kg-day","mg/kg","mg/m3"),
                  study_type %in% c("acute", "short-term", "subchronic",
                                    "chronic", "reproduction developmental",
                                    "developmental")) %>%
    dplyr::mutate(toxval_numeric_log10 = log10(toxval_numeric)) %>%
    dplyr::group_by(toxval_units) %>%
    dplyr::summarize(`25_quants` = quantile(toxval_numeric_log10, probs = c(0.25)) %>%
                       round(., 1),
                     `75_quants` = quantile(toxval_numeric_log10, probs = c(0.75)) %>%
                       round(., 1))


  ### toxval_type_supercategory, risk_assessment_class summary
  # Initial query (count by supersource, supercategory, and RAC)
  toxvaldb_data = runQuery(paste0("SELECT DISTINCT a.supersource, b.toxval_type_supercategory, ",
                                  "a.risk_assessment_class, count(*) as n ",
                                  "FROM toxval a ",
                                  "LEFT JOIN toxval_type_dictionary b ",
                                  "ON a.toxval_type = b.toxval_type ",
                                  "WHERE b.toxval_type_supercategory in ('Toxicity Value', 'Dose Response Summary Value', ",
                                  "'Media Exposure Guidelines', 'Mortality Response Summary Value', ",
                                  "'Acute Exposure Guidelines') and ",
                                  "a.risk_assessment_class not in ('Developmental/Reproductive') and ",
                                  "a.qc_status not like '%fail%' ",
                                  "GROUP BY a.supersource, b.toxval_type_supercategory, a.risk_assessment_class ",
                                  "ORDER BY a.supersource, b.toxval_type_supercategory, a.risk_assessment_class"),
                           toxval.db) %>%
    dplyr::mutate(
      # Fix supercategory presentation
      toxval_type_supercategory = dplyr::case_when(
        toxval_type_supercategory == "Dose Response Summary Value" ~ "DRSV",
        toxval_type_supercategory == "Mortality Response Summary Value" ~ "MRSV",
        toxval_type_supercategory == "Toxicity Value" ~ "Toxicity Values",
        TRUE ~ toxval_type_supercategory
      ),
      # Replace - with "N/A"
      risk_assessment_class = dplyr::case_when(
        risk_assessment_class == "-" ~ "N/A",
        TRUE ~ risk_assessment_class
      )
    )

  # Get color gradient values
  # Unique sequence of count values
  brks_qc = sort(unique(toxvaldb_data$n))
  # Function to map a gradient between colors
  ## For row background
  clrs_rmp_qc <- colorRampPalette(c("white", "black"))
  clrs_df_qc = clrs_rmp_qc(length(unique(toxvaldb_data$n)))
  ## For text font
  clrs_rmp_qc_f <- colorRampPalette(c("black", "white"))
  clrs_df_qc_f <- ifelse(brks_qc > median(brks_qc), "#FFFFFF", "#000000")
  ## Supersource groups
  src_brks = unique(toxvaldb_data$supersource)
  clrs_src = ifelse(src_brks %in% c("Alaska DEC", "Cal OEHHA", "MA DEP", "PA DEP"), "#B2FBA5",
                    ifelse(src_brks %in% c("ECHA IUCLID", "EFSA OpenFooxTox", "GESTIS DNEL",
                                           "Health Canada", "NITE HESS", "WHO IPCS", "WHO JECFA"), "#FAC898",
                           ifelse(src_brks %in% c("Copper Manufacturers", "HAWC Project", "Uterotrophic Hershberger DB"), "#fbf07b",
                                  "#ADD8E6")))

  # Prepare columns, have to be united with a "."
  toxvaldb_data = toxvaldb_data %>%
    # dplyr::mutate(n = prettyNum(n, big.mark = ",")) %>%
    tidyr::unite(
      col = "supcat_rac",
      sep = ".",
      toxval_type_supercategory, risk_assessment_class
    ) %>%
    tidyr::pivot_wider(
      id_cols = supersource,
      names_from = supcat_rac,
      values_from = n
    ) %>%
    dplyr::rename(`Risk assessment class: ` = supersource)

  # Header Columns
  header_cols = c(
    "Risk assessment class: ",
    "DRSV.N/A",
    "MRSV.N/A",
    "Toxicity Values.Cancer",
    "Toxicity Values.Non-cancer",
    "Media Exposure Guidelines.Water",
    "Media Exposure Guidelines.Air",
    "Media Exposure Guidelines.Soil",
    "Acute Exposure Guidelines.Water",
    "Acute Exposure Guidelines.Air",
    "Acute Exposure Guidelines.Soil"
  )

  # Fill blank header cols
  toxvaldb_data[, header_cols[!header_cols %in% names(toxvaldb_data)]] <- NA

  # Select to get column order correct
  toxvaldb_data = toxvaldb_data %>%
    dplyr::select(dplyr::all_of(header_cols))

  # # https://cran.r-project.org/web/packages/ztable/vignettes/heatmapTable.html
  # library(ztable)

  # DT Table Approach - sketch out the layout based on column names
  # https://stackoverflow.com/questions/22897852/how-to-create-datatable-with-complex-header-in-r-shiny
  # https://rstudio.github.io/DT/
  # Add border style
  top_border_style = "text-align: center; border-top-width: 1px; border-top-style: solid; border-top-color: black"
  bottom_border_style = "text-align: center; border-bottom-width: 1px; border-bottom-style: solid; border-bottom-color: black"
  sketch = htmltools::withTags(table(
    class = "row-border",
    thead(
      tr(
        th(rowspan = 2, "Risk assessment class: ", style = paste0(top_border_style, "; ", bottom_border_style)),
        th(colspan = 1, "DRSV", style = top_border_style),
        th(colspan = 1, "MRSV", style = top_border_style),
        th(colspan = 2, "Toxicity Values", style = top_border_style),
        th(colspan = 3, "Media Exposure Guidelines", style = top_border_style),
        th(colspan = 3, "Acute Exposure Guidelines", style = top_border_style),
      ),
      tr(
        th("N/A", style = paste0(top_border_style, "; ", bottom_border_style)),
        th("N/A", style = paste0(top_border_style, "; ", bottom_border_style)),
        th("Cancer", style = paste0(top_border_style, "; ", bottom_border_style)),
        th("Non-cancer", style = paste0(top_border_style, "; ", bottom_border_style)),
        lapply(rep(c("Water", "Air", "Soil"), 2), th, style = paste0(top_border_style, "; ", bottom_border_style))
      )
    )
  ))

  # Create data.table
  dl_fig_list[["Source Record Count by Effect Type Supercategory-Risk Assessment Class"]] = DT::datatable(toxvaldb_data,
                      container = sketch,
                      rownames = FALSE,
                      class = list(stripe = FALSE),
                      options = list(
                        # # Remove pagination
                        # paging = FALSE,
                        # # Remove sort icons
                        # ordering = FALSE,
                        # # Remove search bar
                        # searching = FALSE,
                        # # Remove "n Records" label at bottom
                        # info = FALSE,
                        # # Display all rows (no scrolling)
                        # pageLength = -1,
                        # Center column values
                        columnDefs = list(list(className = 'dt-center',
                                               targets = 1:(length(names(toxvaldb_data))-1)
                        )
                        )
                      )
  ) %>%
    DT::formatStyle(columns = names(toxvaldb_data)[!names(toxvaldb_data) %in% "Risk assessment class: "],
                    # backgroundColor = DT::styleInterval(brks_qc, clrs_df_qc)
                    backgroundColor = DT::styleEqual(brks_qc, clrs_df_qc),
                    color = DT::styleEqual(brks_qc, clrs_df_qc_f),
    ) %>%
    # Format numbers with comma
    DT::formatCurrency(columns = names(toxvaldb_data)[!names(toxvaldb_data) %in% "Risk assessment class: "],
                       currency = "",
                       interval = 3,
                       mark = ",",
                       digits = 0) %>%
    DT::formatStyle(
      "Risk assessment class: ",
      backgroundColor = DT::styleEqual(
        src_brks, clrs_src
      )
    ) %>%
    DT::formatStyle(
      columns = names(toxvaldb_data),
      borderBottomColor = "black",
      borderBottomStyle = "solid",
      borderBottomWidth = "1px"
    )

  # toxicological_effect_category summary
  # Pull all toxval data
  toxval_data = runQuery(paste0("SELECT source_hash, study_type, exposure_route, source FROM toxval ",
                            "WHERE toxval_type in (SELECT toxval_type FROM toxval_type_dictionary WHERE ",
                            "toxval_type_supercategory in ('Dose Response Summary Value', 'Mortality Response Summary Value'))"),
                     toxval.db)

  toxicological_eff_cat = runQuery(paste0("SELECT source_hash, toxicological_effect_category ",
                                          "FROM toxicological_effect_terms"),
                                   toxval.db) %>%
    dplyr::filter(source_hash %in% toxval_data$source_hash) %>%
    dplyr::left_join(toxval_data, by = "source_hash") %>%
    # dplyr::count(study_type, toxicological_effect_category) %>%
    dplyr::filter(study_type %in% c("short-term", "subchronic", "chronic"))

  # Consistent palette colors across plots
  # https://stackoverflow.com/questions/53506536/need-specific-coloring-in-ggplot2-with-viridis
  # Named vector of colors by source_name, add "Other"
  tox_eff_cat_fill_pal <- viridisLite::viridis(length(unique(toxicological_eff_cat$toxicological_effect_category)),
                                               option = "H") %T>%
    { names(.) <- unique(toxicological_eff_cat$toxicological_effect_category) }

  dl_fig_list[["Record Count by Study Type Toxicological Effect Category"]] =
    toxicological_eff_cat %>%
    ggplot2::ggplot(ggplot2::aes(study_type,
                                 fill=toxicological_effect_category,
                                 label=toxicological_effect_category)) +
    ggplot2::geom_bar() +
    ggplot2::theme_minimal(base_size = global_font_size) +
    # ggplot2::theme(text = ggplot2::element_text(size = 14)) +
    ggplot2::guides(fill=ggplot2::guide_legend(ncol=2)) +
    ggplot2::theme(axis.text.x = ggplot2::element_text(angle=45,
                                                       vjust=1,
                                                       hjust=1,
                                                       size=global_font_size),
                   axis.text.y = ggplot2::element_text(size = global_font_size),
                   text = ggplot2::element_text(size = global_font_size)) +
    ggplot2::labs(x = "Study Type",
                  y = "Record Count",
                  fill = "Toxicological Effect Category") +
    ggplot2::scale_fill_manual(values=tox_eff_cat_fill_pal, drop=TRUE) +
    # ggplot2::scale_fill_viridis_d(option="H") +
    # ggplot2::scale_fill_manual(values=unname(pals::trubetskoy())) +
    # ggplot2::scale_fill_manual(values = dichromat::dichromat(viridisLite::turbo(12), type = "protan")) +
    ggplot2::scale_y_continuous(label = scales::comma)

################################################################################
  if(save_png){
    for(fig in names(dl_fig_list)){
      message("Saving figure '", fig, "' (", which(fig == names(dl_fig_list)), " of ", length(names(dl_fig_list)), ")")
      if("datatables" %in% class(dl_fig_list[[fig]])){
        # Save HTML
        DT::saveWidget(dl_fig_list[[fig]], paste0(outputDir, fig, ".html"))
      } else if(grepl("Combined", fig)){
        ggplot2::ggsave(filename = paste0(outputDir, fig, ".png"),
                        plot = dl_fig_list[[fig]],
                        width = 2*png_width,
                        height = 2* png_height,
                        units = png_units)
      } else if (fig == "Chemical Class Boxplots") {
        ggplot2::ggsave(filename = paste0(outputDir, fig, ".png"),
                        plot = dl_fig_list[[fig]],
                        width = 1.5 * png_width,
                        height = 1.5 * png_height,
                        units = png_units)
      } else {
        ggplot2::ggsave(filename = paste0(outputDir, fig, ".png"),
                        plot = dl_fig_list[[fig]],
                        width = png_width,
                        height = png_height,
                        units = png_units)
      }

    }
  }

  # Commented out logic for exporting HTML as PNG
  # Before running, update DT options() to remove search, pagination, etc. that is commented out
  # https://stackoverflow.com/questions/60287652/how-to-save-a-table-i-created-with-dt-datatable-into-a-high-quality-image
  # DT::saveWidget(dl_fig_list$`Source Record Count by Effect Type Supercategory-Risk Assessment Class`, "dtable.html")
  # webshot::webshot("dtable.html", "dtable.png")

  # Prep captions
  dl_fig_captions = list(
    `Record Count by Effect Type Supercategory` = 'Count of records by source within each Effect Type Supercategory. Sources with fewer than 3,000 records are grouped into "Other".',
    `Chemical Count by Effect Type Supercategory` = 'Count of unique chemicals by source within each Effect Type Supercategory. Sources with fewer than 1,000 chemicals are grouped into "Other".',
    `Effect Type Supercategory Bar Plot` = effects_other_caption,
    `Species by Study Type and Effect Type Category Counts` = "Evaluation by species for select study types for records within the Effect Type Supercategory Dose Response Summary Value (DRSV) showing record counts. Only species with more than 1,000 records are shown.",
    `Species by Study Type and Effect Type Category DRSV Box Plots` = "Evaluation by species for select study types for records within the Effect Type Supercategory Dose Response Summary Value (DRSV) showing the distribution of DRSVs by study type and species. Only species with more than 1,000 records are shown.",
    `Chemical Class Boxplots` = paste0("Distribution of oral dose response summary values (DRSV) in mg/kg-d by structure class, showing the ", sum(chem_class_cap$n)," most potent structure classes. Boxes are colored by the main structure class types with lower and upper hinges corresponding to the first and third quartiles. Whiskers show the range within 1.5 times the interquartile range. Points represent outliers. The red line is logged median across all oral DRSVs for all chemicals in mg/kg-day. ",
                                       "The ", sum(chem_class_cap$n), " most potent class types by median DRSV included: ",
                                       toString(paste0(chem_class_cap$n[1:nrow(chem_class_cap)-1], " ", chem_class_cap$class_type[1:nrow(chem_class_cap)-1])),
                                       " and ", chem_class_cap$n[nrow(chem_class_cap)], " ", chem_class_cap$class_type[nrow(chem_class_cap)], "."
                                       ),
    `Oral/Inhalation Density Plots` = paste0("Distribution of oral and inhalation effect levels with units of mg/kg (oral), mg/kg-day (oral), and mg/m3 (inhalation). Included records are limited to dose response and mortality response summary values. ",
                                             "The log10 25th and 75th percentiles are ",
                                             toString(paste0(density_percentiles$`25_quants`[1:nrow(density_percentiles)-1], " and ",
                                                             density_percentiles$`75_quants`[1:nrow(density_percentiles)-1], " for studies in ",
                                                             density_percentiles$toxval_units[1:nrow(density_percentiles)-1])),
                                             ", and ", density_percentiles$`25_quants`[nrow(density_percentiles)], " and ",
                                             density_percentiles$`75_quants`[nrow(density_percentiles)], " for studies in ",
                                             density_percentiles$toxval_units[nrow(density_percentiles)],
                                             ", respectively."),
    `Source Record Count by Effect Type Supercategory-Risk Assessment Class` = paste0(
      "Summary of available values for each source by Effect Type Supercategory",
      " and Risk Assessment Class. Source name cell color corresponds to source type association ",
      "(green = state agency; blue = federal; orange = international; yellow = other/non-governmental)."
    ),
    `Record Count by Study Type Toxicological Effect Category` = paste0(
      "Summary of records with Toxicological Effect Category assignments by study type. Note, a single record can have multiple category assignments due to having multiple toxicological effect terms."
    )
  )

  return(
    list(plots = dl_fig_list,
         captions = dl_fig_captions)
  )
}
