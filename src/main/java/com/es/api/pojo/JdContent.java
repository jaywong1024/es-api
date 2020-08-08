package com.es.api.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class JdContent {
    private String title;
    private String price;
    private String img;
    private String shop;
    private String icon;
}
