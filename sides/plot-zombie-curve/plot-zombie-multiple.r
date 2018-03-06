require("ggplot2")
require("jsonlite")
require("reshape2")
require("plyr")

GB = 2^30
MB = 2^20
blocksize = 128*2^10

load_data <- function(file_path) {
    json_data = fromJSON(txt=file_path)
    return(json_data[['ftl_func_valid_ratios']])
}

extract_one_snapshot1 <- function(snapshots) {
    # get only the last snapshot
    dd = tail(snapshots, 1)

    dd = melt(as.matrix(dd))
    names(dd) = c('snapshot_id', 'valid_ratio', 'count1')
    dd = subset(dd, is.na(count1) == FALSE)
    dd$snapshot_id = NULL

    return(dd)
}

extract_one_snapshot2 <- function(snapshots) {
    # get only the last snapshot
    dd = tail(snapshots, 1)

    dd = melt(as.matrix(dd))
    names(dd) = c('snapshot_id', 'valid_ratio', 'count2')
    dd = subset(dd, is.na(count2) == FALSE)
    dd$snapshot_id = NULL

    return(dd)
}

organize_data1 <- function(d) {
    d = arrange(d, desc(valid_ratio))
    d = transform(d, seg_end = cumsum(count1))
    d = transform(d, seg_start = seg_end - count1)
    d = melt(d, 
         measure = c('seg_start', 'seg_end'), value.name = 'blocknum1')
    d = arrange(d, desc(valid_ratio))

    d = transform(d, block_location = (as.numeric(blocknum1)/(16*MB)) * as.numeric(blocksize))
    d = subset(d, valid_ratio != 0)

    return(d)
}

organize_data2 <- function(d) {
    d = arrange(d, desc(valid_ratio))
    d = transform(d, seg_end = cumsum(count2))
    d = transform(d, seg_start = seg_end - count2)
    d = melt(d, 
         measure = c('seg_start', 'seg_end'), value.name = 'blocknum2')
    d = arrange(d, desc(valid_ratio))

    d = transform(d, block_location = (as.numeric(blocknum2)/(16*MB)) * as.numeric(blocksize))
    d = subset(d, valid_ratio != 0)

    return(d)
}

plot_old <- function(d) {
    p = ggplot(d, aes(x = block_location, y = valid_ratio)) +
        geom_line() +
        ylab('Valid Ratio') +
        xlab('Cumulative Block Ratio')
    print(p)

    ggsave("plot.pdf", plot = p, height = 4, width = 4)
}

plot <- function(d1, d2) {
    # # Combine the data d1, d2
    # d1.names <- names(d1)
    # d2.names <- names(d2)

    # # columns in d1 but not in d2
    # d2.add <- setdiff(d1.names, d2.names)

    # # columns in d2 but not in d1
    # d1.add <- setdiff(d2.names, d1.names)

    # # add blank columns to d2
    # if(length(d2.add) > 0) {
    #     for(i in 1:length(d2.add)) {
    #       d2[d2.add[i]] <- NA
    #     }
    # }

    # # Add blank columns to d1
    # if(length(d1.add) > 0) {
    #     for(i in 1:length(d1.add)) {
    #         d1[d1.add[i]] <- NA
    #     }
    # }

    # d_combined = rbind(d1, d2)

    p = ggplot(d1, aes(x = block_location, y=valid_ratio)) +
        geom_line(colour="green") +
        geom_line(data=d2, colour="red") +
        # geom_line(aes(y=valid_ratio), colour="red") +
        # geom_line(aes(y=valid_ratio), colour="green") +
        ylab('Valid Ratio') +
        xlab('Cumulative Block Ratio in Logical Block (16MB)')

    print(p)

    ggsave("plot_combined.pdf", plot = p, height = 4, width = 4)
}


main <- function() {
    print("Hello")

    snapshots1 = load_data("./recorder1.json")
    d1 = extract_one_snapshot1(snapshots1)
    d1 = organize_data1(d1)

    snapshots2 = load_data("./recorder2.json")
    d2 = extract_one_snapshot2(snapshots2)
    d2 = organize_data2(d2)

    plot(d1, d2)
}

main()

